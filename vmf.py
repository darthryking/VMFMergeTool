"""

vmf.py

Contains the definitions for a VMF, and helper functions for manipulating VMFs 
and VMF objects.

"""

import copy
from collections import OrderedDict

from vdfutils import parse_vdf, format_vdf, VDFConsistencyError
from vmfdelta import (
    VMFDelta,
    AddObject, RemoveObject, ChangeObject, 
    AddProperty, RemoveProperty, ChangeProperty, 
    TieSolid, UntieSolid,
    AddOutput, RemoveOutput,
)


class InvalidVMF(Exception):
    """ The given VMF is not valid. """
    
    def __init__(self, path, message):
        self.path = path if path else '(No Path)'
        self.message = message
        
    def __str__(self):
        return "Invalid VMF file: {}\n{}".format(self.path, self.message)
        
        
class VMF(object):
    """ Class that represents a Valve Map File. """
    
    class ObjectDoesNotExist(Exception):
        ''' The given VMF object doesn't exist. '''
        
        def __init__(self, vmfClass, id):
            self.vmfClass = vmfClass
            self.objectId = id
            
        def __str__(self):
            return "Object with class '{}' and id {} does not exist!".format(
                    self.vmfClass,
                    self.objectId,
                )
                
    EXTENSION = '.vmf'
    
    WORLD = 'world'
    SOLID = 'solid'
    SIDE = 'side'
    ENTITY = 'entity'
    
    CLASSES = (WORLD, SOLID, SIDE, ENTITY)
    
    # Used to delimit sub-property paths. We choose a sequence containing 
    # at least one double quote, because the double quote is the only human-
    # readable character that I know of that is universally disallowed in all 
    # VMF fields, which means we run no risk of accidentally clobbering (or 
    # getting clobbered by) someone's data.
    PROPERTY_DELIMITER = '"::"'
    
    @classmethod
    def from_path(cls, path):
        ''' Returns a constructed VMF from the given *.vmf file path.
        
        If this cannot be done, raises InvalidVMF.
        
        '''
        
        if not path.endswith(VMF.EXTENSION):
            raise InvalidVMF(path, "Invalid file extension!")
            
        with open(path, 'r') as f:
            data = f.read()
            
        try:
            vmfData = parse_vdf(data, allowRepeats=True, escape=False)
        except VDFConsistencyError:
            raise InvalidVMF(path, "Failed to parse VMF!")
            
        return cls(vmfData, path)
        
    def __init__(self, vmfData, path=None):
        self.vmfData = vmfData
        self.path = path
        
        self.lastIdDict = {}
        
        self.revision = int(vmfData['versioninfo']['mapversion'])
        
        # The dictionaries map IDs to VMF objects.
        # 'world' is just the world VMF object.
        self.world = None
        self.solidDict = OrderedDict()
        self.sideDict = OrderedDict()
        self.entityDict = OrderedDict()
        
        # Relates Solid IDs to Entity IDs, for the purpose of keeping track of 
        # brush-based entities.
        self.brushEntityDict = OrderedDict()
        
        # We keep a reference between all objects' identifying information and 
        # their parents' identifying information (if they have one), so that 
        # we can correctly look up an object's parent if we need to create an 
        # AddObject delta.
        #
        # Note: We look up parent information in this dictionary via tuple 
        # pairs containing the target object's information, in the form of 
        # (vmfClass, id). We store parent information in the same format.
        #
        self.parentDict = {}
        
        def update_last_id(vmfClass, id):
            if vmfClass in VMF.CLASSES:
                try:
                    lastId = self.lastIdDict[vmfClass]
                except KeyError:
                    self.lastIdDict[vmfClass] = id
                else:
                    if id > lastId:
                        self.lastIdDict[vmfClass] = id
                        
        def add_solids_from_object(vmfClass, vmfObject):
            if (VMF.SOLID not in vmfObject or
                    isinstance(vmfObject[VMF.SOLID], basestring)):
                return
                
            solids = vmfObject[VMF.SOLID]
            if isinstance(solids, dict):
                solids = [solids]
                
            assert isinstance(solids, list)
            
            for solid in solids:
                solidId = get_id(solid)
                self.solidDict[solidId] = solid
                
                if vmfClass == VMF.ENTITY:
                    self.brushEntityDict[solidId] = get_id(vmfObject)
                    
                self.parentDict[(VMF.SOLID, solidId)] = (
                    vmfClass,
                    get_id(vmfObject),
                )
                
                update_last_id(VMF.SOLID, solidId)
                
                assert VMF.SIDE in solid
                assert isinstance(solid[VMF.SIDE], list)
                
                for side in solid[VMF.SIDE]:
                    sideId = get_id(side)
                    self.sideDict[sideId] = side
                    
                    self.parentDict[(VMF.SIDE, sideId)] = (VMF.SOLID, solidId)
                    
                    update_last_id(VMF.SIDE, sideId)
                    
        for vmfClass, value in vmfData.iteritems():
            if vmfClass == VMF.WORLD:
                assert isinstance(value, dict)
                self.world = value
                
                update_last_id(VMF.WORLD, get_id(value))
                
                add_solids_from_object(vmfClass, value)
                
            elif vmfClass == VMF.ENTITY:
                if isinstance(value, dict):
                    value = [value]
                    
                assert isinstance(value, list)
                
                for entity in value:
                    id = get_id(entity)
                    self.entityDict[id] = entity
                    
                    update_last_id(VMF.ENTITY, id)
                    
                    add_solids_from_object(vmfClass, entity)
                    
        if self.world is None:
            raise InvalidVMF(self.path, "VMF has no world entry!")
            
    def write_path(self, path):        
        ''' Saves this VMF to the given path. '''
        
        outData = format_vdf(self.vmfData, escape=False)
        
        with open(path, 'w') as f:
            f.write(outData)
            
    def get_solid(self, id):
        try:
            return self.solidDict[id]
        except KeyError:
            raise VMF.ObjectDoesNotExist(VMF.SOLID, id)
            
    def get_side(self, id):
        try:
            return self.sideDict[id]
        except KeyError:
            raise VMF.ObjectDoesNotExist(VMF.SIDE, id)
            
    def get_entity(self, id):
        try:
            return self.entityDict[id]
        except KeyError:
            raise VMF.ObjectDoesNotExist(VMF.ENTITY, id)
            
    def get_object(self, vmfClass, id):
        return {
            VMF.WORLD   :   lambda id: self.world,
            VMF.SOLID   :   self.get_solid,
            VMF.SIDE    :   self.get_side,
            VMF.ENTITY  :   self.get_entity,
        }[vmfClass](id)
        
    def has_object(self, vmfClass, id):
        return id in {
            VMF.WORLD   :   {get_id(self.world) : self.world},
            VMF.SOLID   :   self.solidDict,
            VMF.SIDE    :   self.sideDict,
            VMF.ENTITY  :   self.entityDict,
        }[vmfClass]
        
    def iter_solids(self):
        return self.solidDict.itervalues()
        
    def iter_sides(self):
        return self.sideDict.itervalues()
        
    def iter_entities(self):
        return self.entityDict.itervalues()
        
    def iter_objects(self):
        ''' Returns an iterator over all the relevant VMF objects in the VMF.
        
        Note that Entities come before Solids and Sides in this iterator. This 
        is because we want to iterate over higher-level container objects 
        before reaching the lower-level container objects. This ensures that 
        higher-level containers can be found and created before reaching 
        lower-level containers that are dependent on those higher-level 
        containers.
        
        '''
        
        return (
            (vmfClass, vmfObject)
            for vmfClass, iterator in (
                        (VMF.WORLD, (self.world,)),
                        (VMF.ENTITY, self.iter_entities()),
                        (VMF.SOLID, self.iter_solids()),
                        (VMF.SIDE, self.iter_sides()),
                    )
                for vmfObject in iterator
        )
        
    def next_available_id(self, vmfClass):
        try:
            self.lastIdDict[vmfClass] += 1
        except KeyError:
            self.lastIdDict[vmfClass] = 1
            
        return self.lastIdDict[vmfClass]
        
    def get_object_parent_info(self, vmfClass, id):
        ''' Returns the identifying information for the parent of the given 
        VMF object, if it has one.
        
        If the VMF object has no parent, returns None.
        
        '''
        
        return self.parentDict.get((vmfClass, id), None)
        
    def add_object_to_data(self, vmfClass, id, parentInfo):
        ''' Adds an object with the specified VMF class and ID to the VMF data 
        under the given parent object.
        
        The 'parentInfo' argument may be either a tuple pair of the form 
        (vmfClass, id), or None. If the argument is None, this method will add 
        the VMF object to the root VMF data (i.e. self.vmfData) and will not 
        update the parent dictionary (i.e. self.parentDict), because it is not 
        necessary to do so.
        
        This method requires that the given target object exist as an entry in 
        one of the self.sideDict, self.solidDict, etc. VMF object dictionaries 
        before invocation.
        
        '''
        
        vmfObject = self.get_object(vmfClass, id)
        
        if parentInfo is None:
            # Special case where we add the VMF object to the root VMF data.
            parent = self.vmfData
            
        else:
            parent = self.get_object(*parentInfo)
            
            # Update the parent dictionary with the object's new parent info.
            self.parentDict[(vmfClass, id)] = parentInfo
            
        assert isinstance(parent, dict)
        
        add_object_entry(parent, vmfClass, vmfObject)
        
        # try:
            # parent[vmfClass].append(vmfObject)
        # except KeyError:
            # parent[vmfClass] = vmfObject
        # except AttributeError:
            # assert isinstance(parent[vmfClass], dict)
            # parent[vmfClass] = [parent[vmfClass], vmfObject]
            
    def remove_object_from_data(self, vmfClass, id):
        ''' Removes an object from its current location in the VMF data.
        
        This does NOT remove the object from the master dictionaries of VMF 
        objects, e.g. self.solidDict, self.sideDict, etc.
        
        '''
        
        vmfObject = self.get_object(vmfClass, id)
        parentInfo = self.get_object_parent_info(vmfClass, id)
        
        if parentInfo is None:
            # Special case where we remove the VMF object from the root VMF 
            # data.
            parent = self.vmfData
            
        else:
            parent = self.get_object(*parentInfo)
            
            # Remove the corresponding entry in the parent dictionary.
            del self.parentDict[(vmfClass, id)]
            
        assert isinstance(parent, dict)
        
        # Remove the object from its current parent.
        objectEntry = parent[vmfClass]
        try:
            objectEntry.remove(vmfObject)
        except AttributeError:
            # The entry is a dict, i.e. it's the last entry. Remove it.
            assert isinstance(objectEntry, dict)
            del parent[vmfClass]
        else:
            if len(objectEntry) == 1:
                # The entry is now a singleton list. Flatten it to simply 
                # refer to the object itself.
                parent[vmfClass] = objectEntry[0]
                
    def apply_deltas(self, deltas):
        ''' Applies the given deltas to the VMF data and increments the VMF 
        revision number.
        
        '''
        
        # Keep track of all objects that have been removed. This way, we can 
        # know when removing a sub-object is redundant (since its parent would 
        # have already been removed).
        removedObjectsInfoSet = set()
        
        for delta in deltas:
            if isinstance(delta, AddObject):
                # Create the new object.
                newObject = OrderedDict(id=delta.id)
                
                # Add the new object to the appropriate object dictionary.
                {
                    VMF.SOLID   :   self.solidDict,
                    VMF.SIDE    :   self.sideDict,
                    VMF.ENTITY  :   self.entityDict,
                }[delta.vmfClass][delta.id] = newObject
                
                # Add the object to the VMF data under its designated parent.
                self.add_object_to_data(
                        delta.vmfClass,
                        delta.id,
                        delta.parent,
                    )
                    
            elif isinstance(delta, RemoveObject):
                parentInfo = self.get_object_parent_info(
                        delta.vmfClass,
                        delta.id,
                    )
                    
                # Only remove the object if its parent hasn't already been 
                # removed.
                # Note: If parentInfo is None, this still works.
                if parentInfo not in removedObjectsInfoSet:
                    self.remove_object_from_data(delta.vmfClass, delta.id)
                    
                # Remove the object from the appropriate object dictionary.
                del {
                    VMF.SOLID   :   self.solidDict,
                    VMF.SIDE    :   self.sideDict,
                    VMF.ENTITY  :   self.entityDict,
                }[delta.vmfClass][delta.id]
                
                # Keep track of everything that we have removed so far.
                removedObjectsInfoSet.add((delta.vmfClass, delta.id))
                
            elif (isinstance(delta, AddProperty) or 
                    isinstance(delta, ChangeProperty)):
                    
                vmfObject = self.get_object(delta.vmfClass, delta.id)
                set_object_property(vmfObject, delta.key, delta.value)
                
            elif isinstance(delta, RemoveProperty):
                vmfObject = self.get_object(delta.vmfClass, delta.id)
                delete_object_property(vmfObject, delta.key)
                
            elif isinstance(delta, AddOutput):
                entity = self.get_object(VMF.ENTITY, delta.entityId)
                
                if 'connections' not in entity:
                    entity['connections'] = OrderedDict()
                    
                add_object_entry(
                        entity['connections'],
                        delta.output,
                        delta.value,
                    )
                    
            elif isinstance(delta, RemoveOutput):
                entity = self.get_object(VMF.ENTITY, delta.entityId)
                
                assert 'connections' in entity
                
                remove_object_entry(
                        entity['connections'],
                        delta.output,
                        delta.value,
                    )
                    
            elif isinstance(delta, TieSolid):
                self.brushEntityDict[delta.solidId] = delta.entityId
                
                self.remove_object_from_data(VMF.SOLID, delta.solidId)
                
                # Add the solid to the specified entity.
                self.add_object_to_data(
                        VMF.SOLID,
                        delta.solidId,
                        (VMF.ENTITY, delta.entityId),
                    )
                    
            elif isinstance(delta, UntieSolid):
                del self.brushEntityDict[delta.solidId]
                
                self.remove_object_from_data(VMF.SOLID, delta.solidId)
                
                # Add the solid to the world.
                self.add_object_to_data(
                        VMF.SOLID,
                        delta.solidId,
                        (VMF.WORLD, get_id(self.world)),
                    )
                    
        # Increment revision number.
        self.revision += 1
        self.vmfData['versioninfo']['mapversion'] = self.revision
        self.world['mapversion'] = self.revision
        
        
def get_id(vmfObject):
    """ Returns the ID of the given VMF object. """
    return int(vmfObject['id'])
    
    
def add_object_entry(vmfObject, key, value):
    """ Adds an entry to the given VMF object. """
    
    try:
        vmfObject[key].append(value)
    except KeyError:
        vmfObject[key] = value
    except AttributeError:
        vmfObject[key] = [vmfObject[key], value]
        
        
def remove_object_entry(vmfObject, key, value):
    """ Removes a particular object entry from the given VMF object. """
    
    objectEntry = vmfObject[key]
    try:
        objectEntry.remove(value)
    except AttributeError:
        # The entry is a dict, i.e. it's the last entry. Remove it.
        assert isinstance(objectEntry, dict)
        del vmfObject[key]
    else:
        if len(objectEntry) == 1:
            # The entry is now a singleton list. Flatten it to simply 
            # refer to the object itself.
            vmfObject[key] = objectEntry[0]
            
            
def object_has_property(vmfObject, property):
    """ Gives whether or not the given VMF object has the given property. """
    
    object = vmfObject
    for key in property.split(VMF.PROPERTY_DELIMITER):
        assert isinstance(object, dict)
        
        if key not in object:
            return False
            
        object = object[key]
        
    else:
        return True
        
        
def get_object_property(vmfObject, property):
    """ Gets the given property from the given VMF object. """
    
    result = vmfObject
    for key in property.split(VMF.PROPERTY_DELIMITER):
        if not isinstance(result, dict):
            raise KeyError(property)
            
        try:
            result = result[key]
        except KeyError:
            raise KeyError(property)
            
    return result
    
    
def set_object_property(vmfObject, property, value):
    """ Sets a property of the given VMF object to the given value. """
    
    propertyPath = property.split(VMF.PROPERTY_DELIMITER)
    
    object = vmfObject
    for key in propertyPath[:-1]:
        if not isinstance(object, dict):
            raise KeyError(property)
            
        if key not in object:
            object[key] = OrderedDict()
            
        object = object[key]
        
    if not isinstance(object, dict):
        raise KeyError(property)
        
    object[propertyPath[-1]] = value
    
    
def delete_object_property(vmfObject, property):
    """ Deletes a property of the given VMF object. Only removes nested 
    pseudo-objects if they end up empty after deletion.
    
    """
    
    propertyPath = property.split(VMF.PROPERTY_DELIMITER)
    
    objectStack = []
    
    object = vmfObject
    for key in propertyPath[:-1]:
        if not isinstance(object, dict):
            raise KeyError(property)
            
        # Keep a stack of sub-objects so we can walk up the stack later and 
        # delete empty objects.
        objectStack.append((key, object))
        
        try:
            object = object[key]
        except KeyError:
            raise KeyError(property)
            
    if not isinstance(object, dict):
        raise KeyError(property)
        
    del object[propertyPath[-1]]
    
    # Remove empty pseudo-objects.
    while objectStack:
        key, object = objectStack.pop()
        
        if len(object[key]) == 0:
            del object[key]
            
            
def iter_properties(vmfObject):
    """ Returns an iterator over all of the given object's properties and 
    sub-properties, in the form of key/value pairs.
    
    Sub-property paths are delimited by VMF.PROPERTY_DELIMITER.
    
    We do not count the 'id' property as an actual property, since we 
    special-case it all over the place.
    
    """
    
    IGNORED_KEYS = (
        'id',
        'mapversion',
        'connections',
    ) + VMF.CLASSES
    
    for key, value in vmfObject.iteritems():
        if key not in IGNORED_KEYS:
            if isinstance(value, basestring) or isinstance(value, list):
                yield (key, value)
                
            elif isinstance(value, dict):
                for subkey, subvalue in iter_properties(value):
                    yield (
                        VMF.PROPERTY_DELIMITER.join((key, subkey)),
                        subvalue,
                    )
                    
            else:
                assert False
                
                
def iter_outputs(entity):
    """ Returns an iterator over all of the given entity's outputs, in the 
    form (outputName, outputValue, outputID).
    
    """
    
    if 'connections' not in entity:
        return
        
    connections = entity['connections']
    
    # Keeps track of how many of each output we have seen so far.
    # Maps (output, value) pairs to an integer count.
    outputValueCountsDict = {}
    
    for output, values in connections.iteritems():
        if isinstance(values, basestring):
            values = [values]
            
        assert isinstance(values, list)
        
        for value in values:
            count = outputValueCountsDict.get((output, value), 0)
            
            yield (output, value, count)
            
            try:
                outputValueCountsDict[(output, value)] += 1
            except KeyError:
                outputValueCountsDict[(output, value)] = 1
                
                
def compare_vmfs(parent, child):
    """ Compares the given two VMFs, and returns a list of VMFDeltas 
    representing the changes required to mutate the parent into the child.
    
    """
    
    assert isinstance(parent, VMF)
    assert isinstance(child, VMF)
    
    class NonLocal:
        ''' Non-local namespace hack. '''
        pass
        
    # The list of VMFDeltas to be returned.
    deltas = []
    
    # Relates the IDs of objects in the child to their corresponding new IDs 
    # as part of the parent, for newly-added objects.
    # Keys are stored in (vmfClass, id) tuple form.
    newObjectIdDict = {}
    
    # Check for new objects
    for vmfClass, childObject in child.iter_objects():
        id = get_id(childObject)
        
        if not parent.has_object(vmfClass, id):
            # Assign a new ID to this new object.
            newId = parent.next_available_id(vmfClass)
            
            # Keep track of the relationship between the child object's ID and 
            # its new ID as part of the parent.
            newObjectIdDict[(vmfClass, id)] = newId
            
            # Get the parent information for the new object.
            newObjectParentInfo = child.get_object_parent_info(vmfClass, id)
            
            # The new object's designated parent object might also be a new 
            # object. If so, correlate it with the proper new ID.
            if newObjectParentInfo in newObjectIdDict:
                newObjectParentId = newObjectIdDict[newObjectParentInfo]
                newObjectParentInfo = (
                    newObjectParentInfo[0],
                    newObjectParentId,
                )
                
            newDelta = AddObject(newObjectParentInfo, vmfClass, newId)
            deltas.append(newDelta)
            
            # Add each of the object's properties as an AddProperty delta.
            for key, value in iter_properties(childObject):
                newDelta = AddProperty(vmfClass, newId, key, value)
                deltas.append(newDelta)
                
            # Add each of the object's outputs as an AddOutput delta, if the 
            # object is an entity.
            if vmfClass == VMF.ENTITY:
                for output, value, outputId in iter_outputs(childObject):
                    newDelta = AddOutput(id, output, value, outputId)
                    deltas.append(newDelta)
                    
    # Check for changed/deleted objects.
    for vmfClass, parentObject in parent.iter_objects():
        id = get_id(parentObject)
        
        try:
            childObject = child.get_object(vmfClass, id)
            
        except VMF.ObjectDoesNotExist:
            # Object was deleted.
            newDelta = RemoveObject(vmfClass, id)
            deltas.append(newDelta)
            continue    # Doing this saves an extra indentation level.
            
        # Be prepared to add the ObjectChanged delta for this particular 
        # VMF object.
        NonLocal.addedObjectChangedDelta = False
        
        def add_object_changed_delta():
            ''' Add the ObjectChanged VMFDelta to the delta list, if we 
            haven't already done so.
            
            '''
            
            if not NonLocal.addedObjectChangedDelta:
                newDelta = ChangeObject(vmfClass, id)
                deltas.append(newDelta)
                NonLocal.addedObjectChangedDelta = True
                
        # Check for new properties.
        for key, value in iter_properties(childObject):
            if not object_has_property(parentObject, key):
                add_object_changed_delta()
                newDelta = AddProperty(
                        vmfClass,
                        id,
                        key,
                        copy.deepcopy(value)
                    )
                deltas.append(newDelta)
                
        # Check for changed/deleted properties.
        for key, value in iter_properties(parentObject):
            try:
                childPropertyValue = get_object_property(childObject, key)
                
            except KeyError:
                # Property was deleted.
                add_object_changed_delta()
                newDelta = RemoveProperty(vmfClass, id, key)
                deltas.append(newDelta)
                continue
                
            if childPropertyValue != value:
                # Property was changed.
                add_object_changed_delta()
                newDelta = ChangeProperty(
                        vmfClass,
                        id,
                        key,
                        copy.deepcopy(childPropertyValue),
                    )
                deltas.append(newDelta)
                
        # Deal with entity I/O if the object is an entity.
        if vmfClass == VMF.ENTITY:
            parentOutputSet = frozenset(iter_outputs(parentObject))
            childOutputSet = frozenset(iter_outputs(childObject))
            
            # Check for new entity outputs.
            for childOutputInfo in childOutputSet:
                if childOutputInfo not in parentOutputSet:
                    add_object_changed_delta()
                    
                    output, value, outputId = childOutputInfo
                    newDelta = AddOutput(id, output, value, outputId)
                    deltas.append(newDelta)
                    
            # Check for deleted entity outputs.
            for parentOutputInfo in parentOutputSet:
                if parentOutputInfo not in childOutputSet:
                    add_object_changed_delta()
                    
                    output, value, outputId = parentOutputInfo
                    newDelta = RemoveOutput(id, output, value, outputId)
                    deltas.append(newDelta)
                    
    # Check for newly-tied solids.
    for solidId, entityId in child.brushEntityDict.iteritems():
        if solidId not in parent.brushEntityDict:
            # Only tie the solid if we don't already have an AddObject delta
            # that adds it as the child of an Entity object.
            if (VMF.SOLID, solidId) not in newObjectIdDict:
                # Retrieve the new Entity's ID as part of the parent.
                newId = newObjectIdDict[(VMF.ENTITY, entityId)]
                
                newDelta = TieSolid(solidId, newId)
                deltas.append(newDelta)
                
    # Check for untied solids.
    for solidId, entityId in parent.brushEntityDict.iteritems():
        if solidId not in child.brushEntityDict:
            newDelta = UntieSolid(solidId)
            deltas.append(newDelta)
            
    # Done!
    return deltas
    
    
def get_parent(vmfs):
    """ From a set of VMFs, determines which one has the lowest map version 
    number, and is therefore the parent.
    
    """
    
    # This avoids the need to subscript and slice.
    vmfs = iter(vmfs)
    
    parent = next(vmfs)
    lowestRevision = parent.revision
    
    for vmf in vmfs:
        revision = vmf.revision
        if revision < lowestRevision:
            parent = vmf
            lowestRevision = revision
            
    return parent
    
    
def load_vmfs(vmfPaths, output=True):
    """ Takes a list of paths to VMF files and returns a list of those VMFs, 
    parsed and ready for processing.
    
    If 'output' is True, writes progress to stdout.
    
    """
    
    vmfs = []
    for i, path in enumerate(vmfPaths):
        if output:
            print "\t* ({}/{}) Loading {}...".format(
                    i + 1,
                    len(vmfPaths),
                    path,
                )
                
        vmfs.append(VMF.from_path(path))
        
    return vmfs
    
    