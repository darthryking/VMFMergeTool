"""

vmfdelta.py

"""

from collections import OrderedDict


class DeltaMergeConflict(Exception):
    """ A conflict was detected while merging deltas. """
    
    def __init__(self, partialDeltas=None):
        self.partialDeltas = partialDeltas
        
    def __str__(self):
        return (
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
        
    def __eq__(self, other):
        ''' Two deltas are equivalent when they represent the same conceptual 
        kind of change as another, without regard to the details of such a 
        change. For instance, two ChangeProperty deltas of the same key on the 
        same object are equal, no matter what their respective values are. 
        This is used for merging and conflict detection.
        
        Two deltas of different types never compare equal.
        
        '''
        
        raise NotImplementedError
        
    def __hash__(self):
        ''' Used to ensure that equivalent deltas are correctly hashed in 
        sets.
        
        '''
        
        raise NotImplementedError
        
        
class AddObject(VMFDelta):
    def __init__(self, parent, vmfClass, id):
        self.parent = parent
        self.vmfClass = vmfClass
        self.id = id
        super(AddObject, self).__init__()
        
    def __repr__(self):
        return "AddObject({}, {}, {})".format(
                repr(self.parent),
                repr(self.vmfClass),
                repr(self.id),
            )
            
    def __eq__(self, other):
        # AddObject deltas are always completely unique.
        return False
        
    def __hash__(self):
        return hash((self.parent, self.vmfClass, self.id))
        
        
class RemoveObject(VMFDelta):
    def __init__(self, vmfClass, id):
        self.vmfClass = vmfClass
        self.id = id
        super(RemoveObject, self).__init__()
        
    def __repr__(self):
        return "RemoveObject({}, {})".format(
                repr(self.vmfClass),
                repr(self.id),
            )
            
    def __eq__(self, other):
        return (
            type(self) is type(other) and 
            self.vmfClass == other.vmfClass and 
            self.id == other.id
        )
        
    def __hash__(self):
        return hash((self.vmfClass, self.id))
        
        
class ChangeObject(VMFDelta):
    def __init__(self, vmfClass, id):
        self.vmfClass = vmfClass
        self.id = id
        super(ChangeObject, self).__init__()
        
    def __repr__(self):
        return "ChangeObject({}, {})".format(
                repr(self.vmfClass),
                repr(self.id),
            )
            
    def __eq__(self, other):
        return (
            type(self) is type(other) and 
            self.vmfClass == other.vmfClass and 
            self.id == other.id
        )
        
    def __hash__(self):
        return hash((self.vmfClass, self.id))
        
        
class AddProperty(VMFDelta):
    def __init__(self, vmfClass, id, key, value):
        self.vmfClass = vmfClass
        self.id = id
        self.key = key
        self.value = value
        super(AddProperty, self).__init__()
        
    def __repr__(self):
        return "AddProperty({}, {}, {}, {})".format(
                repr(self.vmfClass),
                repr(self.id),
                repr(self.key),
                repr(self.value),
            )
            
    def __eq__(self, other):
        return (
            type(self) is type(other) and 
            self.vmfClass == other.vmfClass and
            self.id == other.id and
            self.key == other.key
        )
        
    def __hash__(self):
        return hash((self.vmfClass, self.id, self.key))
        
        
class RemoveProperty(VMFDelta):
    def __init__(self, vmfClass, id, key):
        self.vmfClass = vmfClass
        self.id = id
        self.key = key
        super(RemoveProperty, self).__init__()
        
    def __repr__(self):
        return "RemoveProperty({}, {}, {})".format(
                repr(self.vmfClass),
                repr(self.id),
                repr(self.key),
            )
            
    def __eq__(self, other):
        return (
            type(self) is type(other) and 
            self.vmfClass == other.vmfClass and
            self.id == other.id and
            self.key == other.key
        )
        
    def __hash__(self):
        return hash((self.vmfClass, self.id, self.key))
        
        
class ChangeProperty(VMFDelta):
    def __init__(self, vmfClass, id, key, value):
        self.vmfClass = vmfClass
        self.id = id
        self.key = key
        self.value = value
        super(ChangeProperty, self).__init__()
        
    def __repr__(self):
        return "ChangeProperty({}, {}, {}, {})".format(
                repr(self.vmfClass),
                repr(self.id),
                repr(self.key),
                repr(self.value),
            )
            
    def __eq__(self, other):
        return (
            type(self) is type(other) and 
            self.vmfClass == other.vmfClass and
            self.id == other.id and
            self.key == other.key
        )
        
    def __hash__(self):
        return hash((self.vmfClass, self.id, self.key))
        
        
class TieSolid(VMFDelta):
    def __init__(self, solidId, entityId):
        self.solidId = solidId
        self.entityId = entityId
        super(TieSolid, self).__init__()
        
    def __repr__(self):
        return "TieSolid({}, {})".format(
                repr(self.solidId),
                repr(self.entityId),
            )
            
    def __eq__(self, other):
        return (
            type(self) is type(other) and 
            self.solidId == other.solidId
        )
        
    def __hash__(self):
        return hash(self.solidId)
        
        
class UntieSolid(VMFDelta):
    def __init__(self, solidId):
        self.solidId = solidId
        super(UntieSolid, self).__init__()
        
    def __repr__(self):
        return "UntieSolid({})".format(
                repr(self.solidId)
            )
            
    def __eq__(self, other):
        return (
            type(self) is type(other) and 
            self.solidId == other.solidId
        )
        
    def __hash__(self):
        return hash(self.solidId)
        
        
class AddOutput(VMFDelta):
    def __init__(self, entityId, output, value, outputId):
        self.entityId = entityId
        self.output = output
        self.value = value
        self.outputId = outputId
        super(AddOutput, self).__init__()
        
    def __repr__(self):
        return "AddOutput({}, {}, {}, {})".format(
                repr(self.entityId),
                repr(self.output),
                repr(self.value),
                repr(self.outputId),
            )
            
    def __eq__(self, other):
        return (
            type(self) is type(other) and
            self.entityId == other.entityId and
            self.output == other.output and
            self.outputId == other.outputId
        )
        
    def __hash__(self):
        return hash((self.entityId, self.output, self.value, self.outputId))
        
        
class RemoveOutput(VMFDelta):
    def __init__(self, entityId, output, value, outputId):
        self.entityId = entityId
        self.output = output
        self.value = value
        self.outputId = outputId
        super(RemoveOutput, self).__init__()
        
    def __repr__(self):
        return "RemoveOutput({}, {}, {}, {})".format(
                repr(self.entityId),
                repr(self.output),
                repr(self.value),
                repr(self.outputId),
            )
            
    def __eq__(self, other):
        return (
            type(self) is type(other) and
            self.entityId == other.entityId and
            self.output == other.output and
            self.outputId == other.outputId
        )
        
    def __hash__(self):
        return hash((self.entityId, self.output, self.value, self.outputId))
        
        
def merge_delta_lists(deltaLists, aggressive=False):
    """ Takes multiple lists of deltas, and merges them into a single list of
    deltas that can be used to mutate the parent VMF into a merged VMF with 
    all the required changes.
    
    If a conflict is detected, raises DeltaMergeConflict, with the exception's 
    'partialDeltas' attribute set to the partial list of merged deltas with 
    the conflicts removed.
    
    """
    
    # The delta types we care about, in the order that we care about.
    deltaTypes = (
        AddObject,
        TieSolid,
        UntieSolid,
        RemoveObject,
        ChangeObject,
        AddProperty,
        RemoveProperty,
        ChangeProperty,
        AddOutput,
        RemoveOutput,
    )
    
    # For keeping track of what deltas have been merged so far.
    # Maps deltas to themselves, so that they can be retrieved for comparison.
    mergedDeltasDict = OrderedDict()
    
    def merge(delta):
        ''' Attempts to merge the given delta into the mergedDeltasDict.
        
        If a merge conflict is detected, emits a warning, discards the 
        conflicting deltas, and raises DeltaMergeConflict.
        
        '''
        
        if isinstance(delta, ChangeObject):
            # Check for conflicts with RemoveObject deltas.
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
                
                # Discard the RemoveObject delta and insert the ChangeObject 
                # delta, because it makes human intervention a bit easier.
                del mergedDeltasDict[removeObjectDelta]
                mergedDeltasDict[delta] = delta
                
                raise DeltaMergeConflict
                
        elif isinstance(delta, AddProperty):
            # Check for conflicts with other AddProperty deltas.
            if delta in mergedDeltasDict:
                other = mergedDeltasDict[delta]
                
                if other.value != delta.value:
                    # Conflict!
                    print (
                        "CONFLICT WARNING: AddProperty conflict detected!"
                    )
                    print '\t', delta
                    print '\t', other
                    
                    # Discard the conflicting delta and don't try to insert 
                    # the current delta into the dictionary.
                    del mergedDeltasDict[other]
                    
                    raise DeltaMergeConflict
                    
        elif isinstance(delta, ChangeProperty):
            # Check for conflicts with RemoveProperty deltas.
            removePropertyDelta = RemoveProperty(
                    delta.vmfClass,
                    delta.id,
                    delta.key,
                )
            if removePropertyDelta in mergedDeltasDict:
                # Conflict!
                print (
                    "CONFLICT WARNING: ChangeProperty delta conflicts "
                    "with RemoveProperty delta!"
                )
                print '\t', delta
                print '\t', other
                
                # Discard the RemoveProperty delta and insert the 
                # ChangeProperty delta, because it makes human intervention a 
                # bit easier.
                del mergedDeltasDict[removePropertyDelta]
                mergedDeltasDict[delta] = delta
                
                raise DeltaMergeConflict
                
            # Check for conflicts with other ChangeProperty deltas.
            if delta in mergedDeltasDict:
                other = mergedDeltasDict[delta]
                
                if other.value != delta.value:
                    # Conflict!
                    print (
                        "CONFLICT WARNING: ChangeProperty conflict "
                        "detected!"
                    )
                    print '\t', delta
                    print '\t', other
                    
                    # Discard the conflicting delta and don't try to insert 
                    # the current delta into the dictionary.
                    del mergedDeltasDict[other]
                    
                    raise DeltaMergeConflict
                    
        elif isinstance(delta, TieSolid):
            # Check for conflicts with other TieSolid deltas.
            if delta in mergedDeltasDict:
                other = mergedDeltasDict[delta]
                
                if other.entityId != delta.entityId:
                    # Conflict!
                    print (
                        "CONFLICT WARNING: TieSolid conflict detected!"
                    )
                    print '\t', delta
                    print '\t', other
                    
                    # Discard the conflicting delta and don't try to insert 
                    # the current delta into the dictionary.
                    del mergedDeltasDict[other]
                    
                    raise DeltaMergeConflict
                    
        # Merge the delta into the dictionary.
        mergedDeltasDict[delta] = delta
        
    ##################
    # End of merge() #
    ##################
    
    # Maps delta types to lists containing all deltas of that type.
    deltaTypeListsDict = OrderedDict(
            (DeltaType, [])
            for DeltaType in deltaTypes
        )
        
    # Build the deltaTypeListsDict.
    for deltas in deltaLists:
        for delta in deltas:
            deltaTypeListsDict[type(delta)].append(delta)
            
    conflicted = False
    for deltaType, deltas in deltaTypeListsDict.iteritems():
        for delta in deltas:
            try:
                merge(delta)
            except DeltaMergeConflict:
                conflicted = True
                
    # The result is simply the list of keys in the mergedDeltasDict.
    mergedDeltas = mergedDeltasDict.keys()
    
    if conflicted:
        raise DeltaMergeConflict(mergedDeltas)
    else:
        return mergedDeltas
        
        