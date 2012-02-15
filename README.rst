Installation
------------

1. Put CSVTOOL_MODELS in settings.py

2. Put csvtool.py somewhere in your project. I like utils/csvtool.py

3. Then in a view initiate the uploader with an app.model string

Example::

    CSVTOOL_MODELS = {'app1.Model1':{'duplicate_entry':'overwrite'},
                      'app2.Model1':{'duplicate_entry':'overwrite'},
                      'app2.Model2':{'duplicate_entry':'add'},
                      'app3.Model1':{'duplicate_entry':'ignore'},                  
                     }