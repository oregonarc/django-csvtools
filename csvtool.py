"""
@author Wil Black
Feb. 13, 2012 

Module to handle all tasks related to table-based importing and exporting data in
Comma Seperated Values format. 


"""
import os
import datetime as dt
import csv as csv_mod

from django.forms import ModelForm
from django.http import HttpResponse

from fish.settings import CSVTOOL_MODELS, DATABASES, ROOT_PATH, TEMP_DIR

REVERT_DT = 1*60*60  # Time in secs 

class CSVTool():
    
    project_name = 'fish'
    MODELS = CSVTOOL_MODELS.keys()
    OPTIONS = CSVTOOL_MODELS
    tformat = "%Y%m%d_%H%M%S"
    
    def __init__(self, app_model):
        """
        app_model [String] in the models list
        APPNAME.MODELNAME (case sensitive)
        
        """   
        self.model = self._get_model(app_model)
        self.app_model = app_model
        self.value = app_model.replace(".","-")
        
        self._get_options()
        self._get_form()
        self._get_fields()
        self._get_expected()
        self._get_docs()
        self._get_lookup_codes()
        self._get_foreign_keys()
        
    
    def qs2response(self, qs):
        """
        Writes a query set in CSV format based on the input queryset, qs.
        Returns an HttpReponse object containing the csv file.
        """
        
        fields = [f['name'] for f in self.fields]
        #fields.pop("entered")
        #fields.pop("modified")
        
        verbose = fields
        body = []
       
        body = ([q.__getattribute__(f) for f in fields] for q in qs)
        out = self._make_csv_response(fields, body)
        
        return out 
    
    def validate_csv(self, csv, duplicate_entry = None):
        """
        Validates the given csv (a file-like object) against the form and 
        returns ['errors'] and ['is_valid']
        
        """
        if not duplicate_entry:
            duplicate_entry = self.options['duplicate_entry']
            
        pkg = {
            'errors': [],
            'is_valid': False,
        }
        
        csv = csv_mod.DictReader(csv)
           
        if not self._validate_headers(csv.fieldnames, pkg):
            return pkg
                
        row = csv.next()
        row_num=1
        while row:
            row_num +=1
            row = self._convert_fk_names(row)
            self._validate_row(row, duplicate_entry, pkg, row_num)
            try:
                row = csv.next()  
            except StopIteration:
                row = False
        
        pkg['is_valid'] = not pkg['errors']
            
        return pkg
    
    
    def save_csv(self, csv, duplicate_entry = None):
        
        """ 
        Takes a CSV file and saves it to the database. 
        id's are handled as follows
        
        1. If id is not present, a new entry will be created. 
        2. If id is present and duplicate_entry = add, it will attempt to add 
           a new entry with a new id.
        3. If id is present and duplicate_entry = ignore, duplicate id's
           are ignored
        4. If id is present and duplicate_entry = overwrite, It will overwrite 
           the existing entry
            
        If duplicate_entry is not entered, use the model default.
        
        Returns a dict with keys
            'row_num':row_num, 
            'msg':rs,
            'created':created,
            'overwritten':overwritten,
            'ignored':ignored
            
        """
                
        if not duplicate_entry:
            duplicate_entry = self.options['duplicate_entry']
        
        backup_file = self._dump_table() 
                        
        csv = csv_mod.DictReader(csv)
        row = csv.next()
        row_num = 1
        count = 0
        created = 0
        overwritten = 0
        ignored = 0
        rs=[]
        write_type = ''
        while row:
            row = self._convert_fk_names(row)
            try:
                row_id = int(row['id'])
            except KeyError:
                row_id = None
            
            if row_id:
                
                try:
                    obj = self.model.objects.get(pk=int(row['id']))
                except self.model.DoesNotExist:
                    obj = None
                    
                if not obj:
                    
                    write_type="Create: Had id = %s and record did not exist" %int(row['id'])
                    instance = self.form(row).save()
                    instance.id=int(row['id'])
                    instance.save()
                    created += 1
                    
                elif not duplicate_entry == "ignore" :
                    
                    write_type="Overwrite: Had id = %s and record existed" %int(row['id'])
                    if duplicate_entry == "overwrite":
                        instance = self.form(row, instance=obj).save()
                        overwritten += 1
                    
                    elif duplicate_entry == "add":
                        row.pop("id")
                        form = self.form(row)
                        if form.is_valid():
                            form.save()
                            created += 1
                        else:
                            raise Exception(form.errors)
                else:
                    write_type="Overwrite: Had id = %s and record existed" %int(row['id'])
                    ignored += 1
                    instance = obj 
            
            else:
                instance = self.form(row).save()
                created += 1
            try:
                row = csv.next()    
            except StopIteration:
                row = False
            row_num +=1
                
        #raise Exception("Want to see ids")
        
        return {'row_num':row_num, 
                'msg':rs,
                'created':created,
                'overwritten':overwritten,
                'ignored':ignored,
                'backup_file':backup_file,
                }
    
                  
    
    def _convert_fk_names(self, row):
        """
        Loops through a given row and converts it foreign key names to attribute names
            
        """
        out = {}
        for name in row:
            if not row[name] and self.is_null(name): 
                row[name] = None
            tmp = name.split("_id")
            if len(tmp) > 1:
                out.update({tmp[0]:row[name]})
                
            else:
                out.update({name:row[name]})
            
        return out
                
    def is_null(self, name):
        for field in self.model._meta.fields:
            if field.null and field.attname == name:
                return True  
    
    def _validate_headers(self, headers, pkg):
        """
        Validates the given headers against the form field attnames. Use this when the file
        is uploaed. The headers will change in convert_fks()
        
        """
        
        # Track headers
        
        if len(headers) == 1 and 'id' not in headers:
            pkg['errors'].append("<strong>%s</strong> is not a valid header.<br /> Did the CSV file get exported correctly?" %headers)
            return False
            
        if not set(headers) >= set(self.expected):
            pkg['errors'].append('headers: <br />%s<br /> did not match expected headers of <br />%s<br />' % (headers, expected))
            return False
        else:
            return True
    
    def _validate_row(self, row, duplicate_entry, pkg, row_num):
        """
        Takes the headers, a row, and a the form and validates the row 
        against the form using the headers
        
        """
        
        form = self.form(row)
            
        try:
            row_id = int(row['id'])
        except KeyError:
            row_id = None
        except TypeError:
            row_id = None
        
        if row_id:
            
            try:
                obj = self.model.objects.get(pk=int(row['id']))
            except self.model.DoesNotExist:
                obj = None
                
            if not obj:
                
                write_type="Create: Had id = %s and record did not exist" %int(row['id'])
                
                if not form.is_valid():
                    pkg['errors'].append({'row':row_num, 'msg':form.errors})
                    return False        
                
            elif not duplicate_entry == "ignore" :
                if duplicate_entry == "overwrite":
                    form = self.form(row, instance = obj)
                    is_valid = form.is_valid()
                    if not form.is_valid():
                        pkg['errors'].append({'row':row_num, 'msg':form.errors})
                        return False
                    
                elif duplicate_entry == "add":
                    row.pop("id")                
                    form = self.form(row)
                    
                    if not form.is_valid(): 
                        pkg['errors'].append({'row':row_num, 'msg':form.errors})
                        return False
            else:
                write_type="Overwrite: Had id = %s and record existed" %int(row['id'])
                instance = obj 
        
        else:
            if not form.is_valid():
                pkg['errors'].append({'row':row_num, 'msg':form.errors})
                return False
         
        """        
        if form.is_valid():
            return True
        else:
            pkg['errors'].append({'row':row_num, 'msg':form.errors})
            return False
        """
        
        
    def _get_model(self, app_model):
        if not app_model in self.MODELS:
            raise NameError("%s is not support." %app_model)
        app_name, model_name = app_model.split(".")
        
        mod = __import__(self.project_name+"."+app_name+".models" , globals(), locals(), -1)
        model = getattr(mod, model_name)
        return model     
    
    def _get_fields(self):
        self.fields = []
        for field in self.model._meta.fields:
            related_model = ''
                        
            if field.__class__.__name__ == 'ForeignKey':
                related_model = field.rel.to.__name__            
            
            out = {'name':field.attname,
                   'db_type':field.db_type(),
                   'lookup_codes':field.choices,
                   'related_model':related_model,
                   'help_text':field.help_text,
                   'not_blank':not field.blank,
                   'not_null':not field.null,
                   }
            
            self.fields.append(out)
    def _get_expected(self):
        self.expected = [field['name'] for field in self.fields if field['not_blank'] and field['not_null'] ]  
    
    def _get_docs(self):
        self.table_doc = self.model.__doc__
        self.general_doc = """
        First choose the table. Then choose the file you want to upload. Note: header names matter but column order does not. Larger files (greater than 1000 rows) make take a while.

        Rows are written to the database with the following rules:

        The uploaded file will first be validated. If it does not pass validation for any reason the entire file will be reject and nothing will be written to the database. A detailed error report will be generated.
        If a row in the CSV file has a value in the 'id' column then the record will overwrite the existing database record if it exists. If a database record with that 'id' does not exist a new record will be created with the 'id'.
        If no 'id' value is given on a row then a new the database entry will be created for that row an the 'id' will automatically be created.
        Time format: mm/dd/yyyy hh:mm:ss
        
        """     
    
    def _get_options(self):
        self.options = self.OPTIONS[self.app_model]
        return self.OPTIONS[self.app_model]
        
    def _get_form(self):
        """
        Will attempt to load a form with the name ModelnameCSVForm from
        the apps forms.py. If it cannot find a form there is will create one 
        with _create_model_form.
        """
        
        app_name, model_name = self.app_model.split(".")
        
        try:
            mod = __import__(self.project_name+"."+app_name+".forms" , globals(), locals(), -1)
            form = getattr(mod, model_name+"CSVForm")
        except: 
            form = self._create_model_form()
        self.form = form
        return self.form
        
    def _get_lookup_codes(self):
        lookups = {}
        for field in self.model._meta.fields:
            if field.choices:
                lookups.update({field.name:field.choices})
        self.lookup_codes = lookups
        return self.lookup_codes    
    
    def _get_foreign_keys(self):
        fks = {}
        for field in self.model._meta.fields:
            if field.__class__.__name__ == 'ForeignKey':
                fks.update({field.name:field.rel.to.__name__})
        self.fks = fks
        return self.fks
        
    def _create_model_form(self):
        """
        Creates a model form for the model. Used if a CSVForm could not
        be found in forms.py for the model. 
        
        """
        global Model
        Model = self.model
        class _ModelForm(ModelForm):
            class Meta:
                model = Model
                
        return _ModelForm
    
    def _get_fname(self):
        now = dt.datetime.now()
        return "%s_%s" %(self.app_model.replace(".","_"), now.strftime(self.tformat) )
    
    def _dump_table(self, fname = None):
        import commands
        
        if not fname:
            fname = self._get_fname()  
                
        dbname =     DATABASES['default']['NAME']
        dbpassword = DATABASES['default']['PASSWORD']
        dbuser =     DATABASES['default']['USER']
        dbtype =     DATABASES['default']['ENGINE'].split(".")[-1]
        dbtable =    self.model._meta.db_table
        
        if dbtype == 'mysql':
            fname = fname + ".sql"
            outfile = os.path.join(TEMP_DIR, fname)
            cmd = "mysqldump -u%s -p%s %s %s > %s" %(dbuser, dbpassword, dbname, dbtable, outfile)
            print cmd
            out = commands.getstatusoutput(cmd)
            return fname
        else:
            raise Exception("%s is not a supported database type." %dbtype)

    def _load_table(self, fname = None):
        import datetime
        import commands
        
        if not fname:
            raise Exception("You must provide a filename.") 
                
        dbname =     DATABASES['default']['NAME']
        dbpassword = DATABASES['default']['PASSWORD']
        dbuser =     DATABASES['default']['USER']
        dbtype =     DATABASES['default']['ENGINE'].split(".")[-1]
        dbtable =    self.model._meta.db_table
        
        if dbtype == 'mysql':
            
            infile = os.path.join(TEMP_DIR,fname)
            cmd = "mysqldump -u%s -p%s %s < %s" %(dbuser, dbpassword, dbname, infile)
            print cmd
            out = commands.getstatusoutput(cmd)
            
        else:
            raise Exception("%s is not a supported database type." %dbtype)
    
    def revert(self, fname):
        # Check time 
        now = dt.datetime.now()
        delta = now - self._fname2dt(fname)
        
        if delta.seconds < REVERT_DT:
            self._load_table(fname)
        else:
            return {'error':"File was older than the allowed revert time limit."}
          
    def _fname2dt(self, fname):
        
        base, ext = fname.split(".")
        app, model, date_string, time_string = base.split("_")
             
        return dt.datetime(int(date_string[0:4]), 
                           int(date_string[4:6]),
                           int(date_string[6:8]),
                           int(time_string[0:2]),
                           int(time_string[2:4]),
                           int(time_string[4:6])
                           )
        
        
        
      
    def _make_csv_response(self, fields, body, fname=None):
        """
        Returns an HttpResponse object filled with the content
        from header = [] and body = [[]] as a csv object. 
            
        """
        if not fname:
            fname = self._get_fname()+".csv"
                  
        # Convert from generator to list
        fields = list(fields)
        body = list(body)
            
        response = HttpResponse(mimetype='text/csv')
        response['Content-Disposition'] = 'attachment; filename=%s' %(fname)
    
        writer = csv_mod.writer(response)
        writer.writerow(fields)  # Write the fields as headers
        for row in body:
            writer.writerow(row)
    
        return response
        
#csv = CSVTool('wcgsi.Track')


    
    