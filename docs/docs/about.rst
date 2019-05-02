Overview
=========

`objectstorage` is a highly optimized library that manages local storage of R objects.
An emphasis is put on performance: both in access and modification time and file size. 

Features:
---------

* Designed to support concurrent use. All inter-process communication done using file system. 

* Objects are stored as rds archives capapble of being read with `readRDS()`. 

* Small objects are boundled together into on archive automatically for best performance. Large objects are keps separate. Threshold is 5kB.

* Supports parallel `xz` compression if the tool `pxz` is available. 

* Saving in the background in forked process.

* Supports first saving into a fast non-compressed format, and replacing it in the background transparently with the compressed version for fastest possible save time and best compression.

* Fast access to objects metadata containing object digest and size using index file. 

* Lazy loading objects via promises.

How to install
=============

```
devtools::install_github('adamryczkowski/objectstorage')
```

How to use it
=============

First thing to remember is that there is no runtime information in memory to avoid race conditions. All information relevant to the specific object storage is kept in so called index file, and consequently the path to the index file is required for each function from this library. Each function call locks the storage index, does something, and then unlocks.

The index stores the name of all the managed objects, their store location, crc, sizes and other attributes. The path to the index file is the only runtime information that is used, due to the distributed nature of the package. The index can contain information about only one file, or arbitrarily large number of them. To create the index use function `create_objectstorage(<path>)`. The path should point the name of the index file. The index must have a filename extension `.rdx`, which will be appended automatically if not present.

```
storage_path<-'/tmp/storage_example'
objectstorage::create_objectstorage(storage_path)
```

Now we are ready to put an object into the new objectstorage. We do that by using function `save_objects`. Each object must have a name associated with it. There are two functions available: one that uses tidy nse (the default) that accepts pairs (object name, object value) as named argument, and the non-nse version that accepts a named list or an environment (environments are a an R data structure semantically similar to named lists, that are optimized to be used as a storage for variables). All examples do not copy the arguments, so they apply to the objects that barely fit the memory.

```
obj_name<-'my_obj2'
objectstorage::save_objects(storage_path, my_obj1=runif(1000), !!obj_name:=runif(1000))
my_obj3<-runif(1000)
objectstorage::save_objects_non_nse(storage_path, objectnames='my_obj3')
objectstorage::save_objects_non_nse(storage_path, list(my_obj4=runif(1000)))
objectstorage::list_runtime_objects(storage_path)
```

Each function accessing the objectstorage returns a `tibble` (a better data.frame) with the current state of the objectstorage. Let us retrieve the object:

```
rm(my_obj) #Remove the object or just start a new R session
my_obj<-objectstorage::get_object(storage_path, 'my_obj1') #Good for a single object only
objectstorage::load_objects(storage_path, c('my_obj1', 'my_obj2')) 
```




```
save_object(x=1:10, b:=1:10)
```


```
reset;firejail --rlimit-as=600000000 R
storage_path<-'/tmp/storage_example'
options(warn=1)
obj_name<-'my_obj2'

#my_obj3<-runif(30000000)
#objectstorage::save_objects(storage_path, my_obj1=runif(30000000))
#objectstorage::save_objects(storage_path, !!obj_name:=runif(30000000))
objectstorage::save_objects_non_nse(storage_path, list(my_obj4=runif(30000000)))

#debug(objectstorage:::calculate.object.digest)
#objectstorage::save_objects_non_nse(storage_path, objectnames='my_obj3')
```

```
reset;firejail --rlimit-as=600000000 R
storage_path<-'/tmp/storage_example'
my_obj<-objectstorage::get_object(storage_path, 'my_obj1')
```



reset;firejail --rlimit-as=271000000 R
options(warn=1)
my_obj3<-runif(15)
storage_path<-'/tmp/storage_example'
#debug(objectstorage:::calculate.object.digest)
objectstorage::save_objects_non_nse(storage_path, objectnames='my_obj3')

