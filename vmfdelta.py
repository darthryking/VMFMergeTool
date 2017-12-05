"""

vmfdelta.py

"""

from collections import OrderedDict
import vmf


class DeltaMergeConflict(Exception):
    """ A conflict was detected while merging deltas. """
    
    def __init__(self, partialDeltas, conflictedDeltas):
        self.partialDeltas = partialDeltas
        self.conflictedDeltas = conflictedDeltas
        
        super(DeltaMergeConflict, self).__init__(
            "WARNING: Merge conflict(s) detected. "
            "Human intervention will be required for conflict resolution."
        )
        
        
class VMFDelta(object):
    """ Abstract base class for VMF delta objects, which represent changes 
    between two VMFs.
    
    """
    
    def __init__(self, originFile=None):
        # The file from which this delta originated.
        self.originFile = originFile
        self._type = self.__class__.__name__
        
    def _equiv_attrs(self):
        ''' Gives a tuple of attributes that matter for the purposes of 
        "equivalence" between deltas of this type.
        
        (See .__eq__() for an explanation of what this means.)
        
        '''
        
        raise NotImplementedError
        
    def __eq__(self, other):
        ''' Two deltas are equivalent when they represent the same conceptual 
        kind of change as another, without regard to the details of such a 
        change. For instance, two ChangeProperty deltas of the same key on the 
        same object are equal, no matter what their respective values are. 
        This is used for merging and conflict detection.
        
        Two deltas of different types never compare equal.
        
        '''
        
        return (
            type(self) is type(other)
            and self._equiv_attrs() == other._equiv_attrs()
        )
        
    def __hash__(self):
        ''' Used to ensure that equivalent deltas are correctly hashed in 
        sets.
        
        '''
        
        return hash(self._equiv_attrs())
        
        
class AddObject(VMFDelta):
    def __init__(self, parent, vmfClass, id, originFile=None):
        self.parent = parent
        self.vmfClass = vmfClass
        self.id = id
        super(AddObject, self).__init__(originFile)
        
    def __repr__(self):
        return "AddObject({}, {}, {})".format(
            repr(self.parent),
            repr(self.vmfClass),
            repr(self.id),
        )
        
    def _equiv_attrs(self):
        return (self.vmfClass, self.id)
        
        
class RemoveObject(VMFDelta):
    def __init__(self, vmfClass, id, originFile=None):
        self.vmfClass = vmfClass
        self.id = id
        super(RemoveObject, self).__init__(originFile)
        
    def __repr__(self):
        return "RemoveObject({}, {})".format(
            repr(self.vmfClass),
            repr(self.id),
        )
        
    def _equiv_attrs(self):
        return (self.vmfClass, self.id)
        
        
class ChangeObject(VMFDelta):
    def __init__(self, vmfClass, id, originFile=None):
        self.vmfClass = vmfClass
        self.id = id
        super(ChangeObject, self).__init__(originFile)
        
    def __repr__(self):
        return "ChangeObject({}, {})".format(
            repr(self.vmfClass),
            repr(self.id),
        )
        
    def _equiv_attrs(self):
        return (self.vmfClass, self.id)
        
        
class AddProperty(VMFDelta):
    def __init__(self, vmfClass, id, key, value, originFile=None):
        self.vmfClass = vmfClass
        self.id = id
        self.key = key
        self.value = value
        super(AddProperty, self).__init__(originFile)
        
    def __repr__(self):
        return "AddProperty({}, {}, {}, {})".format(
            repr(self.vmfClass),
            repr(self.id),
            repr(self.key),
            repr(self.value),
        )
        
    def _equiv_attrs(self):
        return (self.vmfClass, self.id, self.key)
        
        
class RemoveProperty(VMFDelta):
    def __init__(self, vmfClass, id, key, originFile=None):
        self.vmfClass = vmfClass
        self.id = id
        self.key = key
        super(RemoveProperty, self).__init__(originFile)
        
    def __repr__(self):
        return "RemoveProperty({}, {}, {})".format(
            repr(self.vmfClass),
            repr(self.id),
            repr(self.key),
        )
        
    def _equiv_attrs(self):
        return (self.vmfClass, self.id, self.key)
        
        
class ChangeProperty(VMFDelta):
    def __init__(self, vmfClass, id, key, value, originFile=None):
        self.vmfClass = vmfClass
        self.id = id
        self.key = key
        self.value = value
        super(ChangeProperty, self).__init__(originFile)
        
    def __repr__(self):
        return "ChangeProperty({}, {}, {}, {})".format(
            repr(self.vmfClass),
            repr(self.id),
            repr(self.key),
            repr(self.value),
        )
        
    def _equiv_attrs(self):
        return (self.vmfClass, self.id, self.key)
        
        
class TieSolid(VMFDelta):
    def __init__(self, solidId, entityId, originFile=None):
        self.solidId = solidId
        self.entityId = entityId
        super(TieSolid, self).__init__(originFile)
        
    def __repr__(self):
        return "TieSolid({}, {})".format(
            repr(self.solidId),
            repr(self.entityId),
        )
        
    def _equiv_attrs(self):
        return (self.solidId,)
        
        
class UntieSolid(VMFDelta):
    def __init__(self, solidId, originFile=None):
        self.solidId = solidId
        super(UntieSolid, self).__init__(originFile)
        
    def __repr__(self):
        return "UntieSolid({})".format(
            repr(self.solidId)
        )
        
    def _equiv_attrs(self):
        return (self.solidId,)
        
        
class AddOutput(VMFDelta):
    def __init__(self, entityId, output, value, outputId, originFile=None):
        self.entityId = entityId
        self.output = output
        self.value = value
        self.outputId = outputId
        super(AddOutput, self).__init__(originFile)
        
    def __repr__(self):
        return "AddOutput({}, {}, {}, {})".format(
            repr(self.entityId),
            repr(self.output),
            repr(self.value),
            repr(self.outputId),
        )
        
    def _equiv_attrs(self):
        return (self.entityId, self.output, self.outputId)
        
        
class RemoveOutput(VMFDelta):
    def __init__(self, entityId, output, value, outputId, originFile=None):
        self.entityId = entityId
        self.output = output
        self.value = value
        self.outputId = outputId
        super(RemoveOutput, self).__init__(originFile)
        
    def __repr__(self):
        return "RemoveOutput({}, {}, {}, {})".format(
            repr(self.entityId),
            repr(self.output),
            repr(self.value),
            repr(self.outputId),
        )
        
    def _equiv_attrs(self):
        return (self.entityId, self.output, self.outputId)
        
        
class MoveVisGroup(VMFDelta):
    def __init__(self, visGroupId, parentId, originFile=None):
        self.visGroupId = visGroupId
        self.parentId = parentId
        super(MoveVisGroup, self).__init__(originFile)
        
    def __repr__(self):
        return "MoveVisGroup({}, {})".format(
            repr(self.visGroupId),
            repr(self.parentId),
        )
        
    def _equiv_attrs(self):
        return (self.visGroupId,)
        
        
class AddToVisGroup(VMFDelta):
    def __init__(self, vmfClass, id, visGroupId, originFile=None):
        self.vmfClass = vmfClass
        self.id = id
        self.visGroupId = visGroupId
        super(AddToVisGroup, self).__init__(originFile)
        
    def __repr__(self):
        return "AddToVisGroup({}, {}, {})".format(
            repr(self.vmfClass),
            repr(self.id),
            repr(self.visGroupId),
        )
        
    def _equiv_attrs(self):
        return (self.vmfClass, self.id, self.visGroupId)
        
        
class RemoveFromVisGroup(VMFDelta):
    def __init__(self, vmfClass, id, visGroupId, originFile=None):
        self.vmfClass = vmfClass
        self.id = id
        self.visGroupId = visGroupId
        super(RemoveFromVisGroup, self).__init__(originFile)
        
    def __repr__(self):
        return "RemoveFromVisGroup({}, {}, {})".format(
            repr(self.vmfClass),
            repr(self.id),
            repr(self.visGroupId),
        )
        
    def _equiv_attrs(self):
        return (self.vmfClass, self.id, self.visGroupId)
        
        
class HideObject(VMFDelta):
    def __init__(self, vmfClass, id, originFile=None):
        self.vmfClass = vmfClass
        self.id = id
        super(HideObject, self).__init__(originFile)
        
    def __repr__(self):
        return "HideObject({}, {})".format(
            repr(self.vmfClass),
            repr(self.id),
        )
        
    def _equiv_attrs(self):
        return (self.vmfClass, self.id)
        
        
class UnHideObject(VMFDelta):
    def __init__(self, vmfClass, id, originFile=None):
        self.vmfClass = vmfClass
        self.id = id
        super(UnHideObject, self).__init__(originFile)
        
    def __repr__(self):
        return "UnHideObject({}, {})".format(
            repr(self.vmfClass),
            repr(self.id),
        )
        
    def _equiv_attrs(self):
        return (self.vmfClass, self.id)
        
        
def merge_delta_lists(deltaLists, aggressive=False):
    """ Takes multiple lists of deltas, and merges them into a single list of
    deltas that can be used to mutate the parent VMF into a merged VMF with 
    all the required changes.
    
    If a conflict is detected, raises DeltaMergeConflict, with the exception's 
    'partialDeltas' attribute set to the partial list of merged deltas with 
    the conflicts removed.
    
    """
    
    # Because otherwise we get circular imports if we use 
    # `from vmf import VMF`. Ugh.
    VMF = vmf.VMF
    
    # The delta types we care about, in the order that we care about.
    deltaTypes = (
        AddObject,
        RemoveObject,
        ChangeObject,
        TieSolid,
        UntieSolid,
        AddProperty,
        RemoveProperty,
        ChangeProperty,
        AddOutput,
        RemoveOutput,
        MoveVisGroup,
        AddToVisGroup,
        RemoveFromVisGroup,
        # HideObject,
        # UnHideObject,
    )
    
    # For keeping track of what deltas have been merged so far.
    # Maps deltas to themselves, so that they can be retrieved for comparison.
    mergedDeltasDict = OrderedDict()
    
    # For keeping track of what deltas are conflicted.
    # Maps deltas to a list of all deltas that are "equal" to that delta, so 
    # we can retrieve all deltas that conflict.
    conflictedDeltasDict = OrderedDict()
    
    def iter_processed_deltas(delta):
        ''' Returns an iterator over all merged and conflicted deltas that are 
        "equivalent" to the given delta.
        
        '''
        
        if delta in mergedDeltasDict:
            yield mergedDeltasDict[delta]
            
        if delta in conflictedDeltasDict:
            for other in conflictedDeltasDict[delta]:
                yield other
                
    def add_conflicted_delta(delta):
        ''' Adds the given delta to the conflictedDeltasDict, and removes it 
        from the mergedDeltasDict (if applicable).
        
        '''
        
        # If the conflicting delta is in the mergedDeltasDict, remove it.
        if delta in mergedDeltasDict and delta is mergedDeltasDict[delta]:
            del mergedDeltasDict[delta]
            
        try:
            conflictedDeltasDict[delta].append(delta)
        except KeyError:
            conflictedDeltasDict[delta] = [delta]
            
    def merge(delta):
        ''' Attempts to merge the given delta into the mergedDeltasDict.
        
        If a merge conflict is detected, emits a warning, and adds the 
        conflicting deltas to conflictedDeltasDict.
        
        '''
        
        if isinstance(delta, ChangeObject):
            # Check for conflicts with RemoveObject deltas.
            # Note that RemoveObject deltas never get added to the 
            # conflictedDeltasDict, so there's no need to use 
            # iter_processed_deltas() here.
            removeObjectDelta = RemoveObject(delta.vmfClass, delta.id)
            if removeObjectDelta in mergedDeltasDict:
                other = mergedDeltasDict[removeObjectDelta]
                
                # Conflict!
                print (
                    "CONFLICT WARNING: ChangeObject delta conflicts with "
                    "RemoveObject delta!"
                )
                print '\t', delta
                print '\t', other
                
                # Keep the RemoveObject delta, and put the ChangeObject delta 
                # into the conflictedDeltasDict.
                add_conflicted_delta(delta)
                return
                
        elif isinstance(delta, AddProperty):
            # Is the corresponding ChangeObject or AddObject delta already 
            # conflicted?
            changeObjectDelta = ChangeObject(delta.vmfClass, delta.id)
            addObjectDelta = AddObject(None, delta.vmfClass, delta.id)
            
            if (changeObjectDelta in conflictedDeltasDict
                    or addObjectDelta in conflictedDeltasDict):
                # If so, this delta is automatically also conflicted.
                add_conflicted_delta(delta)
                return
                
            # Otherwise, check for conflicts with other AddProperty deltas.
            for other in iter_processed_deltas(delta):
                if other.value == delta.value:
                    # Save an indent level.
                    continue
                    
                # Conflict!
                print (
                    "CONFLICT WARNING: AddProperty conflict detected!"
                )
                print '\t', delta
                print '\t', other
                
                add_conflicted_delta(delta)
                add_conflicted_delta(other)
                return
                
        elif isinstance(delta, ChangeProperty):
            # Is the corresponding ChangeObject delta already conflicted?
            changeObjectDelta = ChangeObject(delta.vmfClass, delta.id)
            if changeObjectDelta in conflictedDeltasDict:
                # If so, this delta is automatically also conflicted.
                add_conflicted_delta(delta)
                return
                
            # If this is a VisGroup delta, check to see if the VisGroup was 
            # removed.
            if delta.vmfClass == VMF.VISGROUP:
                removeVisGroupDelta = RemoveObject(VMF.VISGROUP, delta.id)
                if removeVisGroupDelta in mergedDeltasDict:
                    # The relevant VisGroup was removed; there's no need to 
                    # add the ChangeProperty delta.
                    return
                    
            # Check for conflicts with RemoveProperty deltas.
            removePropertyDeltas = iter_processed_deltas(
                RemoveProperty(
                    delta.vmfClass,
                    delta.id,
                    delta.key,
                )
            )
            
            try:
                other = next(removePropertyDeltas)
            except StopIteration:
                # Don't do anything if there are no RemoveProperty deltas.
                pass
            else:
                # Conflict!
                print (
                    "CONFLICT WARNING: ChangeProperty delta conflicts "
                    "with RemoveProperty delta!"
                )
                print '\t', delta
                print '\t', other
                
                add_conflicted_delta(delta)
                add_conflicted_delta(other)
                return
                
            # Check for conflicts with other ChangeProperty deltas.
            for other in iter_processed_deltas(delta):
                if other.value == delta.value:
                    # Save an indent level.
                    continue
                    
                # Conflict!
                print (
                    "CONFLICT WARNING: ChangeProperty conflict "
                    "detected!"
                )
                print '\t', delta
                print '\t', other
                
                add_conflicted_delta(delta)
                add_conflicted_delta(other)
                return
                
        elif isinstance(delta, TieSolid):
            # Is the corresponding ChangeObject delta already conflicted?
            changeObjectDelta = ChangeObject(VMF.SOLID, delta.solidId)
            if changeObjectDelta in conflictedDeltasDict:
                # If so, this delta is automatically also conflicted.
                add_conflicted_delta(delta)
                
                # We need to also mark the corresponding AddObject delta for 
                # the entity as conflicted (if the entity is new).
                addEntityDelta = AddObject(None, VMF.ENTITY, delta.entityId)
                if addEntityDelta in mergedDeltasDict:
                    actualAddEntityDelta = mergedDeltasDict[addEntityDelta]
                    add_conflicted_delta(actualAddEntityDelta)
                    
                return
                
            # Check for conflicts with other TieSolid deltas.
            for other in iter_processed_deltas(delta):
                if other.entityId == delta.entityId:
                    # Save an indent level.
                    continue
                    
                # Conflict!
                print (
                    "CONFLICT WARNING: TieSolid conflict detected!"
                )
                print '\t', delta
                print '\t', other
                
                add_conflicted_delta(delta)
                add_conflicted_delta(other)
                
                # We need to also mark the corresponding AddObject delta for 
                # the entity as conflicted (if the entity is new).
                addEntityDelta = AddObject(None, VMF.ENTITY, delta.entityId)
                if addEntityDelta in mergedDeltasDict:
                    actualAddEntityDelta = mergedDeltasDict[addEntityDelta]
                    add_conflicted_delta(actualAddEntityDelta)
                    
                return
                
        elif isinstance(delta, MoveVisGroup):
            # Check to see if the VisGroup was removed.
            removeVisGroupDelta = RemoveObject(VMF.VISGROUP, delta.visGroupId)
            if removeVisGroupDelta in mergedDeltasDict:
                # The relevant VisGroup was removed; there's no need to add 
                # this delta.
                return
                
        elif isinstance(delta, AddToVisGroup):
            # Check to see if the VisGroup was removed, or if the relevant
            # object was removed.
            removeVisGroupDelta = RemoveObject(VMF.VISGROUP, delta.visGroupId)
            removeObjectDelta = RemoveObject(delta.vmfClass, delta.id)
            
            if (removeVisGroupDelta in mergedDeltasDict
                    or removeObjectDelta in mergedDeltasDict):
                # The relevant VisGroup/object was removed; there's no need to 
                # add this delta.
                return
                
        # Merge the delta into the dictionary.
        mergedDeltasDict[delta] = delta
        
    ##################
    # End of merge() #
    ##################
    
    # Maps delta types to lists containing all deltas of that type.
    deltasForDeltaType = OrderedDict(
        (DeltaType, [])
        for DeltaType in deltaTypes
    )
    
    # Build the deltasForDeltaType dict.
    for deltas in deltaLists:
        for delta in deltas:
            deltasForDeltaType[type(delta)].append(delta)
            
    # Merge!
    for deltas in deltasForDeltaType.itervalues():
        for delta in deltas:
            merge(delta)
            
    # The result is simply the list of keys in the mergedDeltasDict.
    mergedDeltas = mergedDeltasDict.keys()
    
    if conflictedDeltasDict:
        # Uh oh, there were conflicts!
        
        # Flatten the conflicts dictionary.
        conflictedDeltas = [
            delta
            for deltas in conflictedDeltasDict.itervalues()
                for delta in deltas
        ]
        
        conflictedDeltas.sort(key=lambda delta: deltaTypes.index(type(delta)))
        
        raise DeltaMergeConflict(mergedDeltas, conflictedDeltas)
        
    else:
        return mergedDeltas
        
        