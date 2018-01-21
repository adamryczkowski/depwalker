There are three different situations, when the code needs to calculate the hash of the task.


1. To assess, whether the task has been fundamentally changed in a way, that its runtime statistics no-longer apply.

One has to compare the following fields of the task, when saving the metadata to disk:
a) input code - both filenames and the contents
b) immidiate parents - not their CRC, but only their location. We don't need to know whether their parents have changed recursevilly.
c) runtime.objects - their CRC and names

2. To assess, whether the cached value is still valid, when deciding whether to run the task or retieve the cached object:
a) input code - both filenames and the contents
b) all parents - by their CRC that include all the information from this paragraph + objectrecords
c) runtime.objects - their CRC and names, using only 
d) optionally objectrecords - their names. Only if this is a parent of the assessed task


