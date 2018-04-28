"""

vmf.py

Contains the definitions for a VMF, and helper functions for manipulating VMFs 
and VMF objects.

"""

import os
import copy
from collections import OrderedDict, deque

from vdfutils import parse_vdf, format_vdf, VDFConsistencyError
from vmfdelta import (
    VMFDelta,
    AddObject, RemoveObject, ChangeObject, 
    AddProperty, RemoveProperty, ChangeProperty, 
    TieSolid, UntieSolid,
    AddOutput, RemoveOutput,
    MoveVisGroup,
    AddToVisGroup, RemoveFromVisGroup,
    HideObject, UnHideObject,
)


class InvalidVMF(Exception):
    """ The given VMF is not valid. """
    
    def __init__(self, path, message):
        self.path = path if path else '(No Path)'
        super(InvalidVMF, self).__init__(message)
        
        
class VMF(object):
    """ Class that represents a Valve Map File. """
    
    class ObjectDoesNotExist(Exception):
        ''' The given VMF object doesn't exist. '''
        
        def __init__(self, vmfClass, id):
            self.vmfClass = vmfClass
            self.objectId = id
            
            super(VMF.ObjectDoesNotExist, self).__init__(
                "Object with class '{}' and id {} does not exist!".format(
                    vmfClass,
                    id,
                )
            )
            
    EXTENSION = '.vmf'
    
    WORLD = 'world'
    SOLID = 'solid'
    SIDE = 'side'
    GROUP = 'group'
    ENTITY = 'entity'
    VISGROUP = 'visgroup'
    
    CLASSES = (WORLD, SOLID, SIDE, GROUP, ENTITY, VISGROUP)
    
    # Used to delimit sub-property paths. We choose a sequence containing 
    # at least one double quote, because the double quote is the only human-
    # readable character that I know of that is universally disallowed in all 
    # VMF fields, which means we run no risk of accidentally clobbering (or 
    # getting clobbered by) someone's data.
    PROPERTY_DELIMITER = '"::"'
    
    # The full property path to an object's 'visgroupid' sub-property.
    VISGROUP_PROPERTY_PATH = PROPERTY_DELIMITER.join(('editor', 'visgroupid'))
    
    # The full property path to an object's 'groupid' sub-property.
    GROUP_PROPERTY_PATH = PROPERTY_DELIMITER.join(('editor', 'groupid'))
    
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
        
        self.lastIdForVmfClass = {}
        
        self.revision = int(vmfData['versioninfo']['mapversion'])
        
        # The dictionaries map IDs to VMF objects.
        # 'world' is just the world VMF object.
        self.world = None
        self.solidsById = OrderedDict()
        self.sidesById = OrderedDict()
        self.groupsById = OrderedDict()
        self.entitiesById = OrderedDict()
        self.visGroupsById = OrderedDict()
        
        # Relates Solid IDs to Entity IDs, for the purpose of keeping track of 
        # brush-based entities.
        self.entityIdForSolidId = OrderedDict()
        
        # We keep a reference between all objects' identifying information and 
        # their parents' identifying information (if they have one), so that 
        # we can correctly look up an object's parent if we need to create an 
        # AddObject delta.
        #
        # Note: We look up parent information in this dictionary via tuple 
        # pairs containing the target object's information, in the form of 
        # (vmfClass, id). We store parent information in the same format.
        #
        self.parentInfoForObject = {}
        
        def update_last_id(vmfClass, id):
            if vmfClass in VMF.CLASSES:
                try:
                    lastId = self.lastIdForVmfClass[vmfClass]
                except KeyError:
                    self.lastIdForVmfClass[vmfClass] = id
                else:
                    if id > lastId:
                        self.lastIdForVmfClass[vmfClass] = id
                        
        def add_solids_from_object(vmfClass, vmfObject):
            if (VMF.SOLID not in vmfObject
                    or isinstance(vmfObject[VMF.SOLID], basestring)):
                return
                
            solids = vmfObject[VMF.SOLID]
            if isinstance(solids, dict):
                solids = [solids]
                
            assert isinstance(solids, list)
            
            for solid in solids:
                solidId = get_id(solid)
                self.solidsById[solidId] = solid
                
                if vmfClass == VMF.ENTITY:
                    self.entityIdForSolidId[solidId] = get_id(vmfObject)
                    
                self.parentInfoForObject[(VMF.SOLID, solidId)] = (
                    vmfClass,
                    get_id(vmfObject),
                )
                
                update_last_id(VMF.SOLID, solidId)
                
                assert VMF.SIDE in solid
                assert isinstance(solid[VMF.SIDE], list)
                
                for side in solid[VMF.SIDE]:
                    sideId = get_id(side)
                    self.sidesById[sideId] = side
                    
                    self.parentInfoForObject[(VMF.SIDE, sideId)] = (
                        VMF.SOLID,
                        solidId,
                    )
                    
                    update_last_id(VMF.SIDE, sideId)
                    
        def iter_visgroups(visGroupsObject):
            try:
                topLevelVisGroups = visGroupsObject[VMF.VISGROUP]
            except KeyError:
                return
                
            if not isinstance(topLevelVisGroups, list):
                topLevelVisGroups = [topLevelVisGroups]
                
            # Breadth-first traversal of the VisGroup tree.
            # We iterate over all tuples of (parent, VisGroup).
            visGroupQ = deque(
                (None, visGroup)    # Top-level VisGroups have no parent.
                for visGroup in topLevelVisGroups
            )
            while visGroupQ:
                parent, visGroup = visGroupQ.popleft()
                
                assert isinstance(parent, dict) or parent is None
                assert isinstance(visGroup, dict)
                
                yield parent, visGroup
                
                try:
                    childVisGroups = visGroup[VMF.VISGROUP]
                except KeyError:
                    continue
                else:
                    if not isinstance(childVisGroups, list):
                        childVisGroups = [childVisGroups]
                        
                    visGroupQ.extend(
                        (visGroup, childVisGroup)
                        for childVisGroup in childVisGroups
                    )
                    
                    assert all(isinstance(x, dict) for _, x in visGroupQ)
                    
        # Add normal VMF objects, e.g. world and entity objects.
        for vmfClass, value in vmfData.iteritems():
            if vmfClass == VMF.WORLD:
                assert isinstance(value, dict)
                self.world = value
                
                worldId = get_id(value)
                update_last_id(VMF.WORLD, worldId)
                
                add_solids_from_object(vmfClass, value)
                
                # Add groups
                if VMF.GROUP not in value:
                    continue
                    
                groups = value[VMF.GROUP]
                if not isinstance(groups, list):
                    groups = [groups]
                    
                for group in groups:
                    groupId = get_id(group)
                    self.groupsById[groupId] = group
                    
                    self.parentInfoForObject[(VMF.GROUP, groupId)] = (
                        vmfClass,
                        worldId,
                    )
                    
                    update_last_id(VMF.GROUP, groupId)
                    
            elif vmfClass == VMF.ENTITY:
                if isinstance(value, dict):
                    value = [value]
                    
                assert isinstance(value, list)
                
                for entity in value:
                    id = get_id(entity)
                    self.entitiesById[id] = entity
                    
                    update_last_id(VMF.ENTITY, id)
                    
                    add_solids_from_object(vmfClass, entity)
                    
        if self.world is None:
            raise InvalidVMF(self.path, "VMF has no world entry!")
            
        # Add VisGroups
        try:
            visGroupsObject = vmfData['visgroups']
        except KeyError:
            return
        else:
            for parent, visGroup in iter_visgroups(visGroupsObject):
                id = get_visgroup_id(visGroup)
                self.visGroupsById[id] = visGroup
                
                if parent is not None:
                    self.parentInfoForObject[(VMF.VISGROUP, id)] = (
                        VMF.VISGROUP,
                        get_visgroup_id(parent),
                    )
                    
                update_last_id(VMF.VISGROUP, id)
                
    def write_path(self, path):        
        ''' Saves this VMF to the given path. '''
        
        outData = format_vdf(self.vmfData, escape=False)
        
        with open(path, 'w') as f:
            f.write(outData)
            
    def get_filename(self):
        return os.path.basename(self.path)
        
    def get_solid(self, id):
        try:
            return self.solidsById[id]
        except KeyError:
            raise VMF.ObjectDoesNotExist(VMF.SOLID, id)
            
    def get_side(self, id):
        try:
            return self.sidesById[id]
        except KeyError:
            raise VMF.ObjectDoesNotExist(VMF.SIDE, id)
            
    def get_group(self, id):
        try:
            return self.groupsById[id]
        except KeyError:
            raise VMF.ObjectDoesNotExist(VMF.GROUP, id)
            
    def get_entity(self, id):
        try:
            return self.entitiesById[id]
        except KeyError:
            raise VMF.ObjectDoesNotExist(VMF.ENTITY, id)
            
    def get_visgroup(self, id):
        try:
            return self.visGroupsById[id]
        except KeyError:
            raise VMF.ObjectDoesNotExist(VMF.VISGROUP, id)
            
    def get_object(self, vmfClass, id):
        return {
            VMF.WORLD       :   lambda id: self.world,
            VMF.SOLID       :   self.get_solid,
            VMF.SIDE        :   self.get_side,
            VMF.GROUP       :   self.get_group,
            VMF.ENTITY      :   self.get_entity,
            VMF.VISGROUP    :   self.get_visgroup,
        }[vmfClass](id)
        
    def has_object(self, vmfClass, id):
        return id in {
            VMF.WORLD       :   {get_id(self.world) : self.world},
            VMF.SOLID       :   self.solidsById,
            VMF.SIDE        :   self.sidesById,
            VMF.GROUP       :   self.groupsById,
            VMF.ENTITY      :   self.entitiesById,
            VMF.VISGROUP    :   self.visGroupsById,
        }[vmfClass]
        
    def iter_solids(self):
        return self.solidsById.itervalues()
        
    def iter_sides(self):
        return self.sidesById.itervalues()
        
    def iter_groups(self):
        return self.groupsById.itervalues()
        
    def iter_entities(self):
        return self.entitiesById.itervalues()
        
    def iter_visgroups(self):
        return self.visGroupsById.itervalues()
        
    def iter_objects(self):
        ''' Returns an iterator over all the relevant VMF objects in the VMF.
        
        Note that Entities come before Solids and Sides in this iterator. This 
        is because we want to iterate over higher-level container objects 
        before reaching the lower-level container objects. This ensures that 
        higher-level containers can be found and created before reaching 
        lower-level containers that are dependent on those higher-level 
        containers.
        
        Also note that we iterate over VisGroups and Groups FIRST. This is to 
        ensure that all new VisGroups and Groups exist before we start adding 
        to them.
        
        '''
        
        return (
            (vmfClass, vmfObject)
            for vmfClass, iterator in (
                        (VMF.VISGROUP, self.iter_visgroups()),
                        (VMF.GROUP, self.iter_groups()),
                        (VMF.WORLD, (self.world,)),
                        (VMF.ENTITY, self.iter_entities()),
                        (VMF.SOLID, self.iter_solids()),
                        (VMF.SIDE, self.iter_sides()),
                    )
                for vmfObject in iterator
        )
        
    def iter_sub_object_infos(self, vmfClass, id):
        """ Returns an iterator over all of the given object's direct 
        sub-objects' infos in (vmfClass, id) form.
        
        This method only returns sub-objects one level deep.
        
        """
        
        vmfObject = self.get_object(vmfClass, id)
        
        if vmfClass in (VMF.WORLD, VMF.ENTITY):
            subObjectClass = VMF.SOLID
            subObjectIdPropName = 'id'
        elif vmfClass == VMF.SOLID:
            subObjectClass = VMF.SIDE
            subObjectIdPropName = 'id'
        elif vmfClass == VMF.VISGROUP:
            subObjectClass = VMF.VISGROUP
            subObjectIdPropName = 'visgroupid'
        elif vmfClass in (VMF.SIDE, VMF.GROUP):
            return
            
        if subObjectClass not in vmfObject:
            return
            
        subObjects = vmfObject[subObjectClass]
        if not isinstance(subObjects, list):
            if isinstance(subObjects, dict):
                subObjects = [subObjects]
            else:
                # This is probably a point entity that happens to have a 
                # "solid" property...
                assert vmfClass == VMF.ENTITY
                assert isinstance(subObjects, basestring)
                return
                
        for subObject in subObjects:
            subObjectId = get_id(subObject, idPropName=subObjectIdPropName)
            yield (subObjectClass, subObjectId)
            
    def next_available_id(self, vmfClass):
        try:
            self.lastIdForVmfClass[vmfClass] += 1
        except KeyError:
            self.lastIdForVmfClass[vmfClass] = 1
            
        return self.lastIdForVmfClass[vmfClass]
        
    def get_object_parent_info(self, vmfClass, id):
        ''' Returns the identifying information for the parent of the given 
        VMF object, if it has one.
        
        If the VMF object has no parent, returns None.
        
        '''
        
        return self.parentInfoForObject.get((vmfClass, id), None)
        
    def add_object_to_data(self, vmfClass, id, parentInfo):
        ''' Adds an object with the specified VMF class and ID to the VMF data 
        under the given parent object.
        
        The 'parentInfo' argument may be either a tuple pair of the form 
        (vmfClass, id), or None. If the argument is None, this method will add 
        the VMF object to the root VMF data (i.e. self.vmfData) and will not 
        update the parent dictionary (i.e. self.parentInfoForObject), because 
        it is not necessary to do so.
        
        This method requires that the given target object exist as an entry in 
        one of the self.sidesById, self.solidsById, etc. VMF object 
        dictionaries before invocation.
        
        '''
        
        vmfObject = self.get_object(vmfClass, id)
        
        if parentInfo is None:
            if vmfClass == VMF.VISGROUP:
                # Special case where we add a VisGroup to the root visgroups 
                # object.
                parent = self.vmfData['visgroups']
            else:
                # Special case where we add the VMF object to the root VMF 
                # data.
                parent = self.vmfData
                
        else:
            parent = self.get_object(*parentInfo)
            
            # Update the parent dictionary with the object's new parent info.
            self.parentInfoForObject[(vmfClass, id)] = parentInfo
            
        assert isinstance(parent, dict)
        
        add_object_entry(parent, vmfClass, vmfObject)
        
    def remove_object_from_data(self, vmfClass, id):
        ''' Removes an object from its current location in the VMF data.
        
        This does NOT remove the object from the master dictionaries of VMF 
        objects, e.g. self.solidsById, self.sidesById, etc.
        
        '''
        
        vmfObject = self.get_object(vmfClass, id)
        parentInfo = self.get_object_parent_info(vmfClass, id)
        
        if parentInfo is None:
            if vmfClass == VMF.VISGROUP:
                # Special case where we remove the VisGroup from the root 
                # VisGroups object.
                parent = self.vmfData['visgroups']
            else:
                # Special case where we remove the VMF object from the root 
                # VMF data.
                parent = self.vmfData
                
        else:
            parent = self.get_object(*parentInfo)
            
            # Remove the corresponding entry in the parent dictionary.
            del self.parentInfoForObject[(vmfClass, id)]
            
        assert isinstance(parent, dict)
        
        remove_object_entry(parent, vmfClass, vmfObject)
        
    def apply_deltas(self, deltas, incrementRevision=True):
        ''' Applies the given deltas to the VMF data and increments the VMF 
        revision number.
        
        '''
        
        # Keep track of all objects that have been removed. This way, we can 
        # know when removing a sub-object is redundant (since its parent would 
        # have already been removed).
        removedObjectsInfoSet = set()
        
        for delta in deltas:
            if isinstance(delta, AddObject):
                # Fix up object IDs, if necessary.
                if delta.id > self.lastIdForVmfClass[delta.vmfClass]:
                    self.lastIdForVmfClass[delta.vmfClass] = delta.id
                    
                # Create the new object.
                if delta.vmfClass == VMF.VISGROUP:
                    newObject = OrderedDict(visgroupid=delta.id)
                else:
                    newObject = OrderedDict(id=delta.id)
                    
                # Add the new object to the appropriate object dictionary.
                {
                    VMF.SOLID       :   self.solidsById,
                    VMF.SIDE        :   self.sidesById,
                    VMF.GROUP       :   self.groupsById,
                    VMF.ENTITY      :   self.entitiesById,
                    VMF.VISGROUP    :   self.visGroupsById,
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
                    VMF.SOLID       :   self.solidsById,
                    VMF.SIDE        :   self.sidesById,
                    VMF.GROUP       :   self.groupsById,
                    VMF.ENTITY      :   self.entitiesById,
                    VMF.VISGROUP    :   self.visGroupsById,
                }[delta.vmfClass][delta.id]
                
                # Keep track of everything that we have removed so far.
                removedObjectsInfoSet.add((delta.vmfClass, delta.id))
                
            elif (isinstance(delta, AddProperty)
                    or isinstance(delta, ChangeProperty)):
                    
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
                self.entityIdForSolidId[delta.solidId] = delta.entityId
                
                self.remove_object_from_data(VMF.SOLID, delta.solidId)
                
                # Add the solid to the specified entity.
                self.add_object_to_data(
                    VMF.SOLID,
                    delta.solidId,
                    (VMF.ENTITY, delta.entityId),
                )
                
            elif isinstance(delta, UntieSolid):
                del self.entityIdForSolidId[delta.solidId]
                
                self.remove_object_from_data(VMF.SOLID, delta.solidId)
                
                # Add the solid to the world.
                self.add_object_to_data(
                    VMF.SOLID,
                    delta.solidId,
                    (VMF.WORLD, get_id(self.world)),
                )
                
            elif isinstance(delta, MoveVisGroup):
                self.remove_object_from_data(VMF.VISGROUP, delta.visGroupId)
                
                parentId = delta.parentId
                
                if parentId is None:
                    newParentInfo = None
                else:
                    newParentInfo = (VMF.VISGROUP, parentId)
                
                self.add_object_to_data(
                    VMF.VISGROUP,
                    delta.visGroupId,
                    newParentInfo,
                )
                
            elif isinstance(delta, AddToVisGroup):
                vmfObject = self.get_object(delta.vmfClass, delta.id)
                
                visGroupsSet = get_object_visgroups(vmfObject)
                visGroupsSet.add(delta.visGroupId)
                
                set_object_visgroups(vmfObject, visGroupsSet)
                
            elif isinstance(delta, RemoveFromVisGroup):
                vmfObject = self.get_object(delta.vmfClass, delta.id)
                
                visGroupsSet = get_object_visgroups(vmfObject)
                visGroupsSet.remove(delta.visGroupId)
                
                set_object_visgroups(vmfObject, visGroupsSet)
                
            elif isinstance(delta, HideObject):
                pass
                
            elif isinstance(delta, UnHideObject):
                pass
                
        if incrementRevision:
            self.increment_revision()
            
    def increment_revision(self):
        """ Increment revision number. """
        self.revision += 1
        self.vmfData['versioninfo']['mapversion'] = self.revision
        self.world['mapversion'] = self.revision
        
    def clone_object_deferred(self, vmfClass, id, cloneIdsDict=None):
        """ Generates and returns a list of deltas that would be sufficient to 
        effectively deep-clone the given object (and its sub-objects).
        
        If `cloneIdsDict` is given, the clone IDs are written to the 
        dictionary, mapping object info in (vmfClass, id) form to the clone's 
        ID.
        
        This cannot be used to clone the World, Groups, or VisGroups.
        
        """
        
        assert vmfClass not in (VMF.WORLD, VMF.GROUP, VMF.VISGROUP)
        
        result = []
        
        vmfObject = self.get_object(vmfClass, id)
        parentInfo = self.get_object_parent_info(vmfClass, id)
        
        # Create a new version of the object.
        newId = self.next_available_id(vmfClass)
        result.append(AddObject(parentInfo, vmfClass, newId))
        
        if cloneIdsDict is not None:
            cloneIdsDict[(vmfClass, id)] = newId
            
        # Add all of the object's properties.
        for key, value in iter_properties(vmfObject):
            result.append(AddProperty(vmfClass, newId, key, value))
            
        # Add all of the object's outputs, if it's an entity.
        if vmfClass == VMF.ENTITY:
            for outputName, outputValue, outputId in iter_outputs(vmfObject):
                result.append(
                    AddOutput(newId, outputName, outputValue, outputId)
                )
                
        # Add all sub-objects.
        for subObjectInfo in self.iter_sub_object_infos(vmfClass, id):
            subObjectClass, subObjectId = subObjectInfo
            subObjectCloneDeltas = self.clone_object_deferred(
                subObjectClass, subObjectId,
                cloneIdsDict=cloneIdsDict,
            )
            
            # Fix up the clones' parent IDs...
            for delta in subObjectCloneDeltas:
                if (isinstance(delta, AddObject)
                        and delta.parent == (vmfClass, id)):
                    delta.parent = (vmfClass, newId)
                    
            result += subObjectCloneDeltas
            
        # Done!
        return result
        
        
def get_id(vmfObject, idPropName='id'):
    """ Returns the ID of the given VMF object. """
    return int(vmfObject[idPropName])
    
    
def get_visgroup_id(vmfObject):
    """ Returns the VisGroup ID of the given VMF object (or the ID of the 
    VisGroup itself, if `vmfObject` is a VisGroup.
    
    """
    
    return get_id(vmfObject, idPropName='visgroupid')
    
    
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
        # The entry is the last entry. Remove it.
        assert (
            isinstance(objectEntry, dict)
            or isinstance(objectEntry, basestring)
        )
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
            
            
def get_object_visgroups(vmfObject):
    """ Get the set of the given object's VisGroups, with IDs in integer form.
    """
    
    try:
        visGroups = get_object_property(
            vmfObject,
            VMF.VISGROUP_PROPERTY_PATH,
        )
    except KeyError:
        return set()
        
    if not isinstance(visGroups, list):
        visGroups = [visGroups]
        
    return set(int(visGroupId) for visGroupId in visGroups)
    
    
def set_object_visgroups(vmfObject, visGroups):
    """ Set the given object's VisGroups. """
    
    visGroups = sorted(str(visGroupId) for visGroupId in visGroups)
    set_object_property(vmfObject, VMF.VISGROUP_PROPERTY_PATH, visGroups)
    
    
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
        # Note that we deal with the 'solid' key a bit specially, since it is 
        # actually a valid property key in non-brush entity objects.
        if (key not in IGNORED_KEYS
                or key == VMF.SOLID and isinstance(value, basestring)):
                
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
    countForOutputValue = {}
    
    for output, values in connections.iteritems():
        if isinstance(values, basestring):
            values = [values]
            
        assert isinstance(values, list)
        
        for value in values:
            count = countForOutputValue.get((output, value), 0)
            
            yield (output, value, count)
            
            try:
                countForOutputValue[(output, value)] += 1
            except KeyError:
                countForOutputValue[(output, value)] = 1
                
                
def compare_vmfs(parent, child):
    """ Compares the given two VMFs, and returns a list of VMFDeltas 
    representing the changes required to mutate the parent into the child.
    
    """
    
    assert isinstance(parent, VMF)
    assert isinstance(child, VMF)
    
    # The list of VMFDeltas to be returned.
    deltas = []
    
    # Classnames of entities that have a "sides" property that refers to a 
    # list of brush faces that will need to be fixed up.
    SIDES_ENTITY_CLASSNAMES = (
        'env_cubemap',
        'info_overlay',
    )
    
    # Cubemap and overlay brush face property deltas that will need to be 
    # fixed up later.
    sidesPropertyDeltas = []
    
    # Relates the IDs of objects in the child to their corresponding new IDs 
    # as part of the parent, for newly-added objects.
    # Keys are stored in (vmfClass, id) tuple form.
    newIdForNewChildObject = {}
    
    def add_visgroup_deltas(vmfClass, id, baseVisGroupIds, childVisGroupIds):
        ''' Take the difference between the given child VisGroup IDs and the 
        given base VisGroup IDs (which are not necessarily in integer form), 
        and create AddToVisGroup/RemoveFromVisGroup deltas as necessary for 
        the given VMF object.
        
        '''
        
        baseVisGroupIds = frozenset(int(id) for id in baseVisGroupIds)
        childVisGroupIds = frozenset(int(id) for id in childVisGroupIds)
        
        newVisGroupIds = childVisGroupIds - baseVisGroupIds
        deletedVisGroupIds = baseVisGroupIds - childVisGroupIds
        
        # Check for new VisGroups
        for visGroupId in newVisGroupIds:
            visGroupInfo = (VMF.VISGROUP, visGroupId)
            
            # Correlate the visGroupId with a new visGroup's ID,
            # if applicable.
            if visGroupInfo in newIdForNewChildObject:
                visGroupId = newIdForNewChildObject[visGroupInfo]
                
            newDelta = AddToVisGroup(vmfClass, id, visGroupId)
            deltas.append(newDelta)
            
        # Check for deleted VisGroups
        for visGroupId in deletedVisGroupIds:
            visGroupInfo = (VMF.VISGROUP, visGroupId)
            newDelta = RemoveFromVisGroup(vmfClass, id, visGroupId)
            deltas.append(newDelta)
            
    # Check for new objects
    for vmfClass, childObject in child.iter_objects():
        id = get_id(
            childObject,
            idPropName='visgroupid' if vmfClass == VMF.VISGROUP else 'id',
        )
        
        if not parent.has_object(vmfClass, id):
            # Assign a new ID to this new object.
            # NOTE: The use of VMF.next_available_id() makes compare_vmfs() an 
            # impure function with side-effects on the parent VMF object.
            # YOU HAVE BEEN WARNED!!!!!
            newId = parent.next_available_id(vmfClass)
            
            # Keep track of the relationship between the child object's ID and 
            # its new ID as part of the parent.
            newIdForNewChildObject[(vmfClass, id)] = newId
            
            # Get the parent information for the new object.
            newObjectParentInfo = child.get_object_parent_info(vmfClass, id)
            
            # The new object's designated parent object might also be a new 
            # object. If so, correlate it with the proper new ID.
            if newObjectParentInfo in newIdForNewChildObject:
                newObjectParentId = newIdForNewChildObject[newObjectParentInfo]
                newObjectParentInfo = (
                    newObjectParentInfo[0],
                    newObjectParentId,
                )
                
            newDelta = AddObject(newObjectParentInfo, vmfClass, newId)
            deltas.append(newDelta)
            
            # Add each of the object's properties.
            for key, value in iter_properties(childObject):
                if vmfClass == VMF.VISGROUP:
                    if key in (VMF.VISGROUP, 'visgroupid'):
                        # Don't add child VisGroup objects as new properties 
                        # of a parent VisGroup, and don't add the 'visgroupid' 
                        # key of a VisGroup object as a new property.
                        continue
                        
                if key == VMF.VISGROUP_PROPERTY_PATH:
                    # Special-case an object's VisGroup properties.
                    if not isinstance(value, list):
                        value = [value]
                        
                    add_visgroup_deltas(vmfClass, newId, [], value)
                    
                else:
                    if key == VMF.GROUP_PROPERTY_PATH:
                        # The group ID needs to be updated with the correct 
                        # group ID as part of the parent.
                        childGroupID = int(value)
                        value = str(
                            newIdForNewChildObject.get(
                                (VMF.GROUP, childGroupID),
                                childGroupID,
                            )
                        )
                        
                    # Add the property as an AddProperty delta.
                    newDelta = AddProperty(vmfClass, newId, key, value)
                    deltas.append(newDelta)
                    
                    # If this is an entity that has a 'sides' property, we'll 
                    # need to fix up its brush face references later.
                    if (vmfClass == VMF.ENTITY
                            and childObject['classname']
                                in SIDES_ENTITY_CLASSNAMES
                            and key == 'sides'):
                        sidesPropertyDeltas.append(newDelta)
                        
            # Add each of the object's outputs as an AddOutput delta, if the 
            # object is an entity.
            if vmfClass == VMF.ENTITY:
                for output, value, outputId in iter_outputs(childObject):
                    newDelta = AddOutput(newId, output, value, outputId)
                    deltas.append(newDelta)
                    
    # Set to keep track of all the ObjectChanged deltas we've added.
    changeObjectDeltaSet = set()
    
    def add_change_object_deltas(vmfClass, id):
        ''' Add the ChangeObject VMFDelta to the delta list for the 
        given object and all of its parents, if we haven't already done 
        so.
        
        '''
        
        if vmfClass == VMF.VISGROUP:
            # Don't add ChangeObject deltas for VisGroups.
            return
            
        objectInfo = (vmfClass, id)
        
        while True:
            vmfClass, id = objectInfo
            
            newDelta = ChangeObject(vmfClass, id)
            
            if newDelta in changeObjectDeltaSet:
                break
                
            changeObjectDeltaSet.add(newDelta)
            deltas.append(newDelta)
            
            parentInfo = parent.get_object_parent_info(vmfClass, id)
            
            if parentInfo is None:
                break
                
            parentClass, parentId = parentInfo
            
            if parentClass == VMF.ENTITY:
                assert vmfClass == VMF.SOLID
                
                if id not in child.entityIdForSolidId:
                    # This solid was untied.
                    # Don't add a ChangeObject delta for the solid's entity.
                    break
                    
            objectInfo = parentInfo
            
    # Check for changed/deleted objects.
    for vmfClass, parentObject in parent.iter_objects():
        id = get_id(
            parentObject,
            idPropName='visgroupid' if vmfClass == VMF.VISGROUP else 'id',
        )
        
        try:
            childObject = child.get_object(vmfClass, id)
        except VMF.ObjectDoesNotExist:
            # Object was deleted.
            childInfos = list(parent.iter_sub_object_infos(vmfClass, id))
            
            if childInfos:
                newDelta = RemoveObject(vmfClass, id, childInfos)
            else:
                newDelta = RemoveObject(vmfClass, id)
                
            deltas.append(newDelta)
            continue    # Doing this saves an extra indentation level.
            
        # If this object is a VisGroup, was it reparented?
        if vmfClass == VMF.VISGROUP:
            parentParentInfo = parent.get_object_parent_info(vmfClass, id)
            childParentInfo = child.get_object_parent_info(vmfClass, id)
            
            if parentParentInfo != childParentInfo:
                # This VisGroup was reparented.
                
                if childParentInfo is None:
                    newParentId = None
                else:
                    _, newParentId = childParentInfo
                    
                newDelta = MoveVisGroup(id, newParentId)
                deltas.append(newDelta)
                
        # All other objects need to get their VisGroup deltas figured out.
        else:
            # Get the parent and child objects' VisGroups
            parentVisGroupIds = get_object_visgroups(parentObject)
            childVisGroupIds = get_object_visgroups(childObject)
            
            # Update the object's VisGroups.
            add_visgroup_deltas(
                vmfClass, id,
                parentVisGroupIds, childVisGroupIds
            )
            
        # Check for new properties.
        for key, value in iter_properties(childObject):
            if key == VMF.VISGROUP_PROPERTY_PATH:
                # We already dealt with VisGroup properties. Ignore them.
                continue
                
            if not object_has_property(parentObject, key):
                add_change_object_deltas(vmfClass, id)
                
                if key == VMF.GROUP_PROPERTY_PATH:
                    # The group ID needs to be updated with the correct group 
                    # ID as part of the parent.
                    childGroupID = int(value)
                    value = str(
                        newIdForNewChildObject[(VMF.GROUP, childGroupID)]
                    )
                    
                newDelta = AddProperty(
                    vmfClass,
                    id,
                    key,
                    copy.deepcopy(value),
                )
                deltas.append(newDelta)
                
                # If this is an entity that has a 'sides' property, we'll need 
                # to fix up its brush face references later.
                if (vmfClass == VMF.ENTITY
                        and childObject['classname'] in SIDES_ENTITY_CLASSNAMES
                        and key == 'sides'):
                    sidesPropertyDeltas.append(newDelta)
                    
        # Check for changed/deleted properties.
        for key, value in iter_properties(parentObject):
            if key == VMF.VISGROUP_PROPERTY_PATH:
                # We already dealt with VisGroup properties. Ignore them.
                continue
                
            try:
                childPropertyValue = get_object_property(childObject, key)
            except KeyError:
                # Property was deleted.
                add_change_object_deltas(vmfClass, id)
                newDelta = RemoveProperty(vmfClass, id, key)
                deltas.append(newDelta)
                continue
                
            if childPropertyValue != value:
                # Property was changed.
                add_change_object_deltas(vmfClass, id)
                
                if key == VMF.GROUP_PROPERTY_PATH:
                    # The group ID needs to be updated with the correct group 
                    # ID as part of the parent.
                    childGroupID = int(childPropertyValue)
                    childPropertyValue = str(
                        newIdForNewChildObject[(VMF.GROUP, childGroupID)]
                    )
                    
                newDelta = ChangeProperty(
                    vmfClass,
                    id,
                    key,
                    copy.deepcopy(childPropertyValue),
                )
                deltas.append(newDelta)
                
                # If this is an entity that has a 'sides' property, we'll need 
                # to fix up its brush face references later.
                if (vmfClass == VMF.ENTITY
                        and childObject['classname'] in SIDES_ENTITY_CLASSNAMES
                        and key == 'sides'):
                    sidesPropertyDeltas.append(newDelta)
                    
        # Deal with entity I/O if the object is an entity.
        if vmfClass == VMF.ENTITY:
            parentOutputSet = frozenset(iter_outputs(parentObject))
            childOutputSet = frozenset(iter_outputs(childObject))
            
            newOutputSet = childOutputSet - parentOutputSet
            deletedOutputSet = parentOutputSet - childOutputSet
            
            # Check for new entity outputs.
            for outputInfo in newOutputSet:
                add_change_object_deltas(vmfClass, id)
                
                output, value, outputId = outputInfo
                newDelta = AddOutput(id, output, value, outputId)
                deltas.append(newDelta)
                
            # Check for deleted entity outputs.
            for outputInfo in deletedOutputSet:
                add_change_object_deltas(vmfClass, id)
                
                output, value, outputId = outputInfo
                newDelta = RemoveOutput(id, output, value, outputId)
                deltas.append(newDelta)
                
    # Check for newly-tied solids.
    for solidId, entityId in child.entityIdForSolidId.iteritems():
        if solidId not in parent.entityIdForSolidId:
            # Only tie the solid if we don't already have an AddObject delta
            # that adds it as the child of an Entity object.
            if (VMF.SOLID, solidId) not in newIdForNewChildObject:
                # Retrieve the new Entity's ID as part of the parent.
                newId = newIdForNewChildObject[(VMF.ENTITY, entityId)]
                
                newDelta = TieSolid(solidId, newId)
                deltas.append(newDelta)
                
        elif parent.entityIdForSolidId[solidId] != entityId:
            # This solid was untied and retied to a different entity.
            # Create an UntieSolid and a TieSolid delta to simulate this.
            newId = newIdForNewChildObject.get(
                (VMF.ENTITY, entityId),
                entityId,
            )
            
            deltas.append(UntieSolid(solidId))
            deltas.append(TieSolid(solidId, newId))
            
    # Check for untied solids.
    for solidId, entityId in parent.entityIdForSolidId.iteritems():
        if solidId not in child.entityIdForSolidId:
            newDelta = UntieSolid(solidId)
            deltas.append(newDelta)
            
    # Fix up cubemap and overlay deltas, which probably point to the wrong 
    # brush faces since we messed with the Side IDs.
    for delta in sidesPropertyDeltas:
        assert (
            isinstance(delta, AddProperty)
            or isinstance(delta, ChangeProperty)
        )
        assert delta.vmfClass == VMF.ENTITY
        assert delta.key == 'sides'
        
        sides = (int(sideIdStr) for sideIdStr in delta.value.split())
        fixedSidesStr = ' '.join(
            str(
                newIdForNewChildObject.get(
                    (VMF.SIDE, sideId),
                    sideId,
                )
            )
            for sideId in sides
        )
        delta.value = fixedSidesStr
        
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
    
    