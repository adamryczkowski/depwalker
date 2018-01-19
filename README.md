# How to use the depwalker

## Metadata creation

First create the metadata file, that contains all the information needed to construct the target object. Use functions:

create.metadata(code | source.path, metadata.path, ...) to create the metadata, and then

add_parent(parent, name,  ...) to add dependencies. These are the objects that will be available for the execution of our code.

add_objectrecord(name, ...) to declare returned objects. Only these objects will be saved.

add_source_file(code | source.path, ...)  to declare additional dependency files that will be tracked along the code and its ancestors. It may be a source file for some additional definitions. Or data source.

After that simply run the `save()` method to actually save the metadata to disk

## Getting the objects

call `get_objects` to get the objects by value, or `load_objects` to place the objects in the chosen environment


# Important design decisions

Metadata must always contain a path to itself. The path can be relative, but it must always be set, even if the metadata itself isn't saved yet.

Only objects with saved to disk metadatas will be allowed to be executed

Code can have side effects, but the system will take care only of the declared objects in the R's memory that are present after execution of the code. All others objects will be assumed to be of temporary nature and no effort will be taken to save them. System does not track any other side effects of running the code.

# Data flow

## load.objects.by.metadata

The highest-level function that is called when getting the object is `load.objects.by.metadata`. This function is ultimately called by each user-facing function for object retrieval: `load.object`, `get.object`, and `get.objects.by.metadata`.

Algorithm

1. Lock the metadata
2. Try to get any of the objects from the memory (from the target.environment) - if exists
3. Try to get any of the objects from hdd (if exists)

If all cached attempts fail: call `create.objects`.

4. Create a run.environment. This is an environment, child of the target.environment, where all the temporary variables will be stored. Results will get moved there.
5. Call create.objects, passing both run.environment and target.environment.

## create.objects

This function starts where load.objects.by.metadata left off. It knows the current object cannot be served by any type of the cache.

1. Load all the parent objects into the run.environment (`load.and.validate.parents`). 
2. Execute the script that places our objects in the run.environment
3. Saves all our objects
4. Promotes all our objects into the target.environment

## load.and.validate.parents

Iterates over all parents (ancestors) and inserts their objects into the environment


TODO:

1. Upewnij się, że w metadatnych są 4 hashe: 1 - hash kodu OK, 2 - hash nazw obiektów runtime (TODO), 3 - hash każdej wartości każdego obiektu (OK)
2. Niech interfejs będzie miał funkcję "Is cached value clean", która zwróci powody, dlaczego cached value jest złe.
OK 3. Dodaj parametr "run.environment", który będzie zawierał obiekty implicite wykorzystywane przez nasz obiekt
OK 4. Niech run.environment może być również listą
5. Niech kod może być podany również jako funkcja. Wówczas do kodu dodamy również wszystkie funkcje wywoływane przez nią, rekurencyjnie
