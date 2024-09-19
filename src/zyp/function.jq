# Support functions for Zyp Transformations
# https://commons-codec.readthedocs.io/zyp/

def to_array:
    # Convert element to array if it isn't an array already.
    if . | type == "array" then
        .
    else
        [.]
    end;

def to_object(options):
    # Wrap element into object with given key if it isn't an object already.
    if . | type == "object" then
        .
    else
        {(options.key): .}
    end;

def is_array_of_objects:
    # Check if element is an array containing objects.
    if (. | type == "array") and (.[0] | type == "object") then
        true
    else
        false
    end;

def del_array_of_objects:
    # If element is an array containing objects, delete it.
    if is_array_of_objects then
        del(.)
    end;

def prune_array_of_objects:
    # Recursively drop arrays of objects.
    walk(del_array_of_objects);

def prune_null:
    # Recursively delete `null` values.
    walk(values);
