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
from fish.wcgsi.models import FishEncounter



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
        """
        fields = [f['name'] for f in self.fields]
        parent_key = self.options['parent_key']
        if parent_key:
            local_field, parent_field = parent_key.split("__")
            fields.remove('id')
            
        raise Exception(parent_key)
        
        #fields.pop("entered")
        #fields.pop("modified")
        
        verbose = fields
        body = []
       
        body = []
        for q in qs:
            row = []
            for f in fields:
                
                if parent_key:
                    obj = q.__getattribute__(local_field)
                    row.append(obj.__getattribute__(parent_field))
                else:
                    row.append(q.__getattribute__(f)) 
            
            body.append(row)
        """
        
        fields, body = self.get_fields_body(qs)
        
        out = self._make_csv_response(fields, body)
        
        return out 
    
    def get_fields_body(self, qs):
        fields = [f['name'] for f in self.fields]
        parent_key = self.options['parent_key']
        local_field = ''
        parent_field = ''
        if parent_key:
            local_field, parent_field = parent_key.split("__")
            fields.remove('id')
        
        body = []
        for q in qs:
            row = []
            for f in fields:
                #
                if local_field+"_id" == f: 
                    obj = q.__getattribute__(local_field)
                    row.append(obj.__getattribute__(parent_field))
                else:
                    row.append(q.__getattribute__(f)) 
            
            body.append(row)
        
        if parent_field:
            index = fields.index(local_field+"_id")
            fields[index] = parent_field
        return fields, body 
                                   
    def validate_csv(self, csv, options = None):
        """
        Validates the given csv (a file-like object) against the form and 
        returns ['errors'] and ['is_valid']
        
        """
                       
        if options:
            for key in self.options:
                if not key in options.keys():
                    options.update({key:self.options[key]})
        
        self.options = options
        
        pkg = {
            'errors': [],
            'is_valid': False,
        }
        
        csv = csv_mod.DictReader(csv)
        try:
            fieldnames = csv.fieldnames
        except:
            pkg['errors'].append("Could not read headers. Please check your file to make sure it has headers in the correct format.")
            pkg['is_valid'] = False
            return pkg
        
        if not self._validate_headers(fieldnames, pkg):
            return pkg
                
        self.created = 0
        self.overwritten = 0
        self.ignored = 0        
        
        try:
            row = csv.next()
        except StopIteration:
            pkg['errors'].append("No rows found. Please check your file and verify it has data in the proper format.")
            pkg['is_valid'] = False
            return pkg
        
        row_num=1
        while row:
            row_num +=1
            row = self._convert_fk_names(row)
            self._validate_row(row, options, pkg, row_num)
            try:
                row = csv.next()  
            except StopIteration:
                row = False
        
        pkg['is_valid'] = not pkg['errors']
            
        return pkg
    
    
    def save_csv(self, csv):
        
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
                

        duplicate_entry = self.options['duplicate_entry']
        pk = self.parent_field or 'id'
           
        backup_file = self._dump_table() 
                        
        csv = csv_mod.DictReader(csv)
        row = csv.next()
        row_num = 1
        count = 0
        self.created = 0
        self.overwritten = 0
        self.ignored = 0
        rs=[]
        write_type = ''
        while row:
            row = self._convert_fk_names(row)
            try:
                row_id = row[self.parent_field]
            except KeyError:
                row_id = None
            
            
            if row_id:
                obj, parent_id = self._get_obj_or_none( row_id )
                
                """
                try:
                    obj = self.model.objects.get(pk=int(row['id']))
                except self.model.DoesNotExist:
                    obj = None
                """    
                if obj:
                    if self.parent_key:
                        row.pop(pk)
                        row.update( {self.local_field+"_id":parent_id })
                                        
                    form = self._get_existing_form(row, obj)                    
                    if form:
                        instance = form.save()
                        if self.parent_key:
                            fe = FishEncounter.objects.get(pk=parent_id)
                            instance.fishencounter = fe
                            instance.save()
                            
                else:    
                    form = self.form(row)
                    form.__setattr__(pk, row_id)
                    form.save()
                    #form = self.form(row)
                    #instance.id=int(row['id'])
                    #instance.save()
                    self.created += 1
                    
            else:
                # Does not have row_id so just created the entry
                instance = self.form(row).save()
                self.created += 1
            try:
                row = csv.next()    
            except StopIteration:
                row = False
            row_num +=1
                
        #raise Exception("Want to see ids")
        
        return {'row_num':row_num, 
                'msg':rs,
                'created':self.created,
                'overwritten':self.overwritten,
                'ignored':self.ignored,
                'backup_file':backup_file,
                }
    
    def revert(self, fname):
        """
        Load the SQL dump file gernerated while saving a CSV but only if the
        filename's time stamp is less than REVERT_DT seconds old. 
        
        Inputs
        ------ 
        fname [STRING] - the filename to be loaded. This filename was gernated by
                         _dumpt_table() and contains the dump stimestamp.
        """ 
        
        now = dt.datetime.now()
        delta = now - self._fname2dt(fname)
        
        if delta.seconds < REVERT_DT:
            self._load_table(fname)
            return {}
        else:
            return {'error':"File was older than the allowed revert time limit."}           
    
    
    def _get_existing_form(self, row, obj):
        """
        Returns an bound form based on the row data and options dict.    
        
        Inputs
        ------
        * row [DICT] - A file row dict generated from the CSV DictReader() method.
        * options [DICT] - Options dict with keys duplicate_entry and parent_key. 
            Keys
            ====
            * duplicate_entry 
            * parent_key - Primary key field name to use as the primary key. If not given
                          it will defualt to 'id'.
        
        
        """
        
        de = self.options['duplicate_entry']
        
        if de == 'overwrite':
            form = self.form(row, instance=obj)
            self.overwritten += 1
        
        elif de == 'add':
            form = self.form(row)
            self.created += 1
        
        elif de == 'ignore':
            form = None
            self.ignored += 1
        
        return form
    
        
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
            pkg['errors'].append('headers: <br />%s<br /> did not match expected headers of <br />%s<br />' % (headers, self.expected))
            return False
        else:
            return True
    
    def _validate_row(self, row, options, pkg, row_num):
        """
        Takes the headers, a row, and a the form and validates the row 
        against the form using the headers
        
        """
        
        #form = self.form(row)
        pk='id'        
        self.parent_key = self.options['parent_key']
        self.local_field = ''
        self.parent_field = ''
        if self.parent_key:
            self.local_field, self.parent_field = self.parent_key.split("__")
            pk=self.parent_field
            
                # So if I want 'barcode' as their primary key I need the fishencounter_id.barcode
        
        try:
            row_id = row[pk]
        except KeyError:
            row_id = None
        except TypeError:
            row_id = None
                
        if row_id:
            try:
                obj, parent_id = self._get_obj_or_none( row_id )
            except:
                pkg['errors'].append({'row':row_num, 
                                      'msg':{self.parent_field:["More than one entry with %s = %s. Cannot overwrite all of them" %(self.parent_field, row_id)]
                                            }
                                      })
                return False
                     
            if obj:
                if self.parent_key:
                    row.pop(pk)
                    row.update( {self.local_field+"_id":parent_id })                
                
                form =self._get_existing_form(row, obj)
                if form:
                    if not form.is_valid():
                        pkg['errors'].append({'row':row_num, 'msg':form.errors})
                        return False 
            else:               
                form = self.form(row)
                form.__setattr__(pk, row_id)
                # fishencount_id = fe.id               
                
                if not form.is_valid():
                    pkg['errors'].append({'row':row_num, 'msg':form.errors})
                    return False        
        
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
        
    def _get_obj_or_none(self, row_id):
        if self.parent_key:
            # Need to generalize this.
            fe = FishEncounter.objects.get(barcode = row_id) 
            parent_id = fe.id
            obj = self.model.objects.filter(fishencounter = fe)
            #raise NameError
            # If more than one object returned, what then?
            if len(obj) > 1:
                if self.options['duplicate_entry'] == 'overwrite':
                    raise Exception("Multiple records found. Cannot overwrite all of them.") 
                else:
                    obj = obj[0]
            elif len(obj) == 1:
                obj = obj[0]
            else:
                obj = None
            
        else:
            parent_id = None
            try:
                obj = self.model.objects.get(pk=int(pk)) 
            except self.model.DoesNotExist:
                obj = None
        return obj, parent_id
        
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
        self.expected = [field['name'] for field in self.fields if field['not_blank'] or field['not_null'] ]  
        
        if self.options['parent_key']:
            tmp = self.options['parent_key'].split("__")
            if not len(tmp) == 2:
                raise Exception("Invlaid parent_key name: %s" %self.options['parent_key']) 
            else:
                self.expected.append(tmp[1])
                self.expected.remove("id")
                if tmp[0]+"_id" in self.expected:
                    self.expected.remove(tmp[0]+"_id")
                
                
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
        if not 'parent_key' in self.options.keys():
            self.options.update({'parent_key':""})
        
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
                
            # Ad if field.foreign_key then and there
            # are less than FK_LOOKUP_MAX entries in the related models
            # make a choices list
            # and return it like is was a choice field.
                
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
            status, output = commands.getstatusoutput(cmd)
            print "Status: ", status
            print "Output: ", output
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
            cmd = "mysql -u%s -p%s %s < %s" %(dbuser, dbpassword, dbname, infile)
            print cmd
            status, output = commands.getstatusoutput(cmd)
            print "Status: ", status
            print "Output: ", output
        else:
            raise Exception("%s is not a supported database type." %dbtype)
    
    
          
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


    
    