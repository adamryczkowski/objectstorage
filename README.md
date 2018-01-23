# objectstorage
Manages local storage of R objects.

Features:

* Objects are stored as rds archives capapble of being read with `readRDS()`. 

* Small objects are boundled together into on archive automatically for best performance. Large objects are keps separate. Threshold is 5kB.

* Supports parallel `xz` compression if the tool `pxz` is available. 

* Saving in the background in forked process.

* Supports first saving into a fast non-compressed format, and replacing it in the background transparently with the compressed version for fastest possible save time and best compression.

* Supports concurrent use, serializes access with file locks.

* Fast access to objects metadata containing object digest and size using index file. 

* Lazy loading objects via promises.
