"""

vmf.py

Contains the definitions for a VMF, and helper functions for manipulating VMFs 
and VMF objects.

"""

from collections import OrderedDict

from vdfutils import parse_vdf, format_vdf, VDFConsistencyError
from vmfdelta import (
    VMFDelta,
    AddObject, RemoveObject, ChangeObject, 
    AddProperty, RemoveProperty, ChangeProperty, 
    TieSolid, UntieSolid,
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
        return (
            (vmfClass, vmfObject)
            for vmfClass, iterator in (
                        (VMF.WORLD, (self.world,)),
                        (VMF.SOLID, self.iter_solids()),
                        (VMF.SIDE, self.iter_sides()),
                        (VMF.ENTITY, self.iter_entities()),
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
        
        try:
            return self.parentDict[(vmfClass, id)]
        except KeyError:
            return None
            
    def clean_data(self):
        ''' Walks through the raw VMF data and removes any objects that have 
        been marked for deletion.
        
        We mark objects for deletion by clearing all their existing entries 
        and adding the special dictionary entry {None : None}.
        
        '''
        
        pass    # TODO: Implement this.
        
    def apply_deltas(self, deltas):
        ''' Applies the given deltas to the VMF data and increments the VMF 
        revision number.
        
        '''
        
        for delta in deltas:
            if isinstance(delta, AddObject):
                # Determine under which parent object this new object should 
                # be added.
                if delta.parent is None:
                    parentObject = self.vmfData
                else:
                    parentObject = self.get_object(*delta.parent)
                    self.parentDict[(delta.vmfClass, delta.id)] = delta.parent
                    
                # Create the new object.
                newObject = OrderedDict(id=delta.id)
                
                # Add the object to its designated parent.
                try:
                    parentObject[delta.vmfClass].append(newObject)
                except KeyError:
                    parentObject[delta.vmfClass] = newObject
                except AttributeError:
                    parentObject[delta.vmfClass] = [
                        parentObject[delta.vmfClass],
                        newObject,
                    ]
                    
                # Add the new object to the appropriate object dictionary.
                {
                    VMF.SOLID   :   self.solidDict,
                    VMF.SIDE    :   self.sideDict,
                    VMF.ENTITY  :   self.entityDict,
                }[delta.vmfClass][delta.id] = newObject
                
            elif isinstance(delta, RemoveObject):
                vmfObject = self.get_object(delta.vmfClass, delta.id)
                
                # Mark the object for deletion.
                vmfObject.clear()
                vmfObject[None] = None
                
            elif (isinstance(delta, AddProperty) or 
                    isinstance(delta, ChangeProperty)):
                
                vmfObject = self.get_object(delta.vmfClass, delta.id)
                vmfObject[delta.key] = delta.value
                
            elif isinstance(delta, RemoveProperty):
                vmfObject = self.get_object(delta.vmfClass, delta.id)
                del vmfObject[delta.key]
                
            elif isinstance(delta, TieSolid):
                self.brushEntityDict[delta.solidId] = delta.entityId
                
                solid = self.get_solid(delta.solidId)
                
                # Get the solid's parent.
                # We directly use the parent dictionary here instead of the 
                # helper method, in order to crash early with an appropriate
                # error message if something absolutely bizarre happens.
                solidParentInfo = self.parentDict[(VMF.SOLID, delta.solidId)]
                solidParent = self.get_object(*solidParentInfo)
                
                # Remove the solid from its original parent.
                solidEntry = solidParent[VMF.SOLID]
                if isinstance(solidEntry, dict):
                    del solidParent[VMF.SOLID]
                elif isinstance(solidEntry, list):
                    solidEntry.remove(solid)
                    
                # Add the solid to the specified entity.
                entity = self.get_entity(delta.entityId)
                try:
                    entity[VMF.SOLID].append(solid)
                except KeyError:
                    entity[VMF.SOLID] = solid
                except AttributeError:
                    entity[VMF.SOLID] = [entity[VMF.SOLID], solid]
                    
                # Update the solid's parent to the entity.
                self.parentDict[(VMF.SOLID, delta.solidId)] = (
                    VMF.ENTITY,
                    get_id(entity),
                )
                
            elif isinstance(delta, UntieSolid):
                del self.brushEntityDict[delta.solidId]
                
                solid = self.get_solid(delta.solidId)
                
                # Get the solid's parent.
                # We directly use the parent dictionary here, for the same 
                # reason as above.
                solidParentInfo = self.parentDict[(VMF.SOLID, delta.solidId)]
                solidParent = self.get_object(*solidParentInfo)
                
                # Remove the solid from its original parent.
                solidEntry = solidParent[VMF.SOLID]
                if isinstance(solidEntry, dict):
                    del solidParent[VMF.SOLID]
                elif isinstance(solidEntry, list):
                    solidEntry.remove(solid)
                    
                # Add the solid to the world.
                try:
                    self.world[VMF.SOLID].append(solid)
                except KeyError:
                    self.world[VMF.SOLID] = solid
                except AttributeError:
                    self.world[VMF.SOLID] = [self.world[VMF.SOLID], solid]
                    
                # Update the solid's parent to the world.
                self.parentDict[(VMF.SOLID, delta.solidId)] = (
                    VMF.WORLD,
                    get_id(self.world),
                )
                
        # TODO: Increment revision number.
        
        
def get_id(vmfObject):
    """ Returns the ID of the given VMF object. """
    return int(vmfObject['id'])
    
    
def iter_properties(vmfObject):
    """ Returns an iterator over all of the given object's properties, in 
    the form of key/value pairs.
    
    We do not count the 'id' property as an actual property, since we 
    special-case it all over the place.
    
    """
    
    return (
        (key, value)
        for key, value in vmfObject.iteritems()
            if key != 'id' and key not in VMF.CLASSES
    )
    
    
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
            
            # The parent might be a new object. If so, correlate it with the 
            # proper new ID.
            if newObjectParentInfo in newObjectIdDict:
                newParentId = newObjectIdDict[newObjectParentInfo]
                newObjectParentInfo = (newObjectParentInfo[0], newParentId)
                
            newDelta = AddObject(newObjectParentInfo, vmfClass, newId)
            deltas.append(newDelta)
            
            # Add each of the object's properties as an AddProperty delta.
            for key, value in iter_properties(childObject):
                newDelta = AddProperty(vmfClass, newId, key, value)
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
            if key not in parentObject:
                add_object_changed_delta()
                newDelta = AddProperty(vmfClass, id, key, value)
                deltas.append(newDelta)
                
        # Check for changed/deleted properties.
        for key, value in iter_properties(parentObject):
            try:
                childPropertyValue = childObject[key]
                
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
                        childPropertyValue,
                    )
                deltas.append(newDelta)
                
    # Check for newly-tied solids.
    for solidId, entityId in child.brushEntityDict.iteritems():
        if solidId not in parent.brushEntityDict:
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
    
    
def load_vmfs(vmfPaths):
    """ Takes a list of paths to VMF files and returns a list of those VMFs, 
    parsed and ready for processing.
    
    """
    
    return [VMF.from_path(path) for path in vmfPaths]
    
    