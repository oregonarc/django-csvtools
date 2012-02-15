Installation
------------

1. Put CSVTOOL_MODELS in settings.py

2. Put csvtool.py somewhere in your project. I like utils/csvtool.py

3. Then in a view initiate the uploader with an app.model string

Example

```CSVTOOL_MODELS = {'wcgsi.Track':{'duplicate_entry':'overwrite',
                              },
                'wcgsi.TrackPoint':{'duplicate_entry':'overwrite',
                                   },
                'wcgsi.FishEncounter':{'duplicate_entry':'add',
                                   },
                'wcgsi.Interview':{'duplicate_entry':'add',
                                   },
                'wcgsi.GSIIndividual':{'duplicate_entry':'ignore',
                              },
                'wcgsi.GSIRun':{'duplicate_entry':'ignore',
                                   },
                  'wcgsi.Genotype':{'duplicate_entry':'ignore',
                                   },
                  'wcgsi.Otolith':{'duplicate_entry':'overwrite',
                                   },
                  'wcgsi.Scale':{'duplicate_entry':'overwrite',
                                   },
                  'wcgsi.Stomach':{'duplicate_entry':'overwrite',
                                   },
                  'profile.Profile':{'duplicate_entry':'overwrite',
                                   },
                  'profile.Vessel':{'duplicate_entry':'overwrite',
                                   },
                  'profile.Port':{'duplicate_entry':'overwrite',
                                   },
                  
                     
               }```