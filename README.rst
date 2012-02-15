Installation
------------

1. Put CSVTOOL_MODELS in settings.py

2. Put csvtool.py somewhere in your project. I like utils/csvtool.py

3. Then in a view initiate the uploader with an app.model string

Add something like this to your settings.py ::

    CSVTOOL_MODELS = {'app1.Model1':{'duplicate_entry':'overwrite'},
                      'app2.Model1':{'duplicate_entry':'overwrite'},
                      'app2.Model2':{'duplicate_entry':'add'},
                      'app3.Model1':{'duplicate_entry':'ignore'},                  
                     }

Duplicate Entries
=================

Duplicate entries are entry that have have the same primary key 
value in both the file and the database table. There are three options to 
deal with duplicate entries.

1. ignore

2. add

3. overwrite

These options are specitfied in CSVTOOL_MODELS in settings.py
