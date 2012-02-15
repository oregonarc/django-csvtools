"""
@author Wil Black
Feb. 13, 2012 

Module to handle all tasks related to table-based importing and exporting data in
Comma Seperated Values format. 


"""
import csv as csv_mod

from django.forms import ModelForm

from fish.settings import CSVTOOL_MODELS, DATABASES, ROOT_PATH 

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
        self._get_docs()
        self._get_lookup_codes()
        self._get_foreign_keys()
        
    
    def validate_csv(self, csv):
        """
        Validates the given csv (a file-like object) against the form and 
        returns ['errors'] and ['is_valid']
        
        """
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
            self._validate_row(row, pkg, row_num)
            try:
                row = csv.next()
                
            except StopIteration:
                row = False
        
        pkg['is_valid'] = not pkg['errors']
            
        return pkg
    
    def save_csv(csv, duplicate_entry = None):
        
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
        """
        
        if not duplicate_entry:
            duplicate_entry = this.options['duplicate_entry']
                
        csv = csv_mod.DictReader(csv)
        row = csv.next()
        row_num = 1
        count = 0
        created = 0
        overwritten = 0
        rs=[]
        write_type = ''
        while row:
            
            try:
                row_id = int(row['id'])
            except ValueError:
                row_id = None
                    
            if row_id:
                
                obj = self.model.objects.get_or_none(pk=int(row['id']))
                if not obj:
                    write_type="Create: Had id = %s and record did not exist" %int(row['id'])
                    instance = form(row).save()
                    instance.id=int(row['id'])
                    instance.save()
                    created += 1
                    
                else:
                    write_type="Overwrite: Had id = %s and record existed" %int(row['id'])
                    if duplicate_entry == "overwrite":
                        instance = form(row, instance=obj).save()
                        overwritten += 1
                    elif duplicate_entry == "add":
                        row.pop("id")
                        instance = form(row).save()
                        created += 1
                    else:
                        ignored += 1 
            else:
                instance = form(row).save()
                created += 1
            
            rs.append({'id':instance.id, 'write_type':write_type})
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
                'ignored':ignored
                }
    
    
    def _validate_headers(headers, pkg):
        """
        Validates the given headers against the form field names
        
        """
        
        # Track headers
        expected = ['id']+self.form().fields.keys()
        if not set(headers) >= set(expected):
            pkg['errors'].append('headers: <br />%s<br /> did not match expected headers of <br />%s<br />' % (headers, expected))
            return False
        else:
            return True
    
    def _validate_row(row, pkg, row_num):
        """
        Takes the headers, a row, and a the form and validates the row 
        against the form using the headers
        
        """
        
        form = self.form(row)
        if form.is_valid():
            return True
        else:
            pkg['errors'].append({'row':row_num, 'msg':form.errors})
            return False
    
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
                related_model = field.related.model.__name__            
            
            out = {'name':field.attname,
                   'db_type':field.db_type(),
                   'lookup_codes':field.choices,
                   'related_model':related_model,
                   'help_text':field.help_text,
                   'not_blank':not field.blank,
                   'not_null':not field.null,
                   }
            
            self.fields.append(out)
            
    
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
        Will attempt to load a form of with name ModelnameCSVForm from
        appname.forms. If it cannot find a form there is will create one 
        with _create_model_form.
        """
        
        app_name, model_name = self.app_model.split(".")
        
        try:
            print "Looking for form"
            mod = __import__(self.project_name+"."+app_name+".forms" , globals(), locals(), -1)
            form = getattr(mod, model_name+"CSVForm")
        except: 
            print "Could not find form. Creating form"
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
                fks.update({field.name:field.related.model.__name__})
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
    
    def _dump_table(self, fname = None):
        import datetime
        import commands
        
        if not fname:
            now = datetime.datetime.now()
            fname = "%s_%s" %(self.app_model.replace(".","_"), now.strftime(self.tformat) )  
                
        dbname =     DATABASES['default']['NAME']
        dbpassword = DATABASES['default']['PASSWORD']
        dbuser =     DATABASES['default']['USER']
        dbtype =     DATABASES['default']['ENGINE'].split(".")[-1]
        dbtable =    self.model._meta.db_table
        
        if dbtype == 'mysql':
            
            cmd = "mysqldump -u%s -p%s %s %s > %s/var/%s.sql" %(dbuser, dbpassword, dbname, dbtable, ROOT_PATH, fname)
            print cmd
            out = commands.getstatusoutput(cmd)
            
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
            
            cmd = "mysqldump -u%s -p%s %s < %s/var/%s.sql" %(dbuser, dbpassword, dbname, ROOT_PATH, fname)
            print cmd
            out = commands.getstatusoutput(cmd)
            
        else:
            raise Exception("%s is not a supported database type." %dbtype)
                 
        
        
#csv = CSVTool('wcgsi.Track')

    