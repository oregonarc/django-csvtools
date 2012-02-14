"""
@author Wil Black
Feb. 13, 2012 

Module to handle all tasks related to table-based importing and exporting data in
Comma Seperated Values format. 


"""


class CSVTool():
    
    project_name = 'fish'
    models = ('wcgsi.Track', 'wcgsi.TrackPoint', 'wcgsi.FishEncounter', 'wcgsi.Interview')
    
    # Duplicate options add, overwrite, ignore
    OPTIONS = {'wcgsi.Track':{'duplicate_entry':'overwrite',
                              },
               'wcgsi.TrackPoint':{'duplicate_entry':'overwrite',
                                   },
               'wcgsi.FishEncounter':{'duplicate_entry':'overwrite',
                                   },
               'wcgsi.Interview':{'duplicate_entry':'overwrite',
                                   },
               }
    
    
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
        
    def _get_model(self, app_model):
        if not app_model in self.models:
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
                   'help_text':field.help_text
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
        app_name, model_name = self.app_model.split(".")
        
        mod = __import__(self.project_name+"."+app_name+".forms" , globals(), locals(), -1)
        form = getattr(mod, model_name+"Form")
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
        
    def _log_event(self):
        pass
    
        
#csv = CSVTool('wcgsi.Track')

    