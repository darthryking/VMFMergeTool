"""

vmfdelta.py

"""


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
    
    def __init__(self):
        # Whether or not we should discard this delta at the end of the delta 
        # merge phase.
        self.shouldDiscard = False
        
    def is_same_change_as(self, other):
        ''' Whether or not this delta represents the same conceptual kind of 
        change as another. Useful for merging and conflict detection.
        
        Assumes that 'other' is of the same type as 'self'.
        
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
            
    def is_same_change_as(self, other):
        # AddObject deltas are always completely unique.
        return False
        
        
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
            
    def is_same_change_as(self, other):
        return self.vmfClass == other.vmfClass and self.id == other.id
        
        
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
            
    def is_same_change_as(self, other):
        return self.vmfClass == other.vmfClass and self.id == other.id
        
        
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
            
    def is_same_change_as(self, other):
        return (
            self.vmfClass == other.vmfClass and
            self.id == other.id and
            self.key == other.key
        )
        
        
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
            
    def is_same_change_as(self, other):
        return (
            self.vmfClass == other.vmfClass and
            self.id == other.id and
            self.key == other.key
        )
        
        
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
            
    def is_same_change_as(self, other):
        return (
            self.vmfClass == other.vmfClass and
            self.id == other.id and
            self.key == other.key
        )
        
        
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
            
    def is_same_change_as(self, other):
        return self.solidId == other.solidId
        
        
class UntieSolid(VMFDelta):
    def __init__(self, solidId):
        self.solidId = solidId
        super(UntieSolid, self).__init__()
        
    def __repr__(self):
        return "UntieSolid({})".format(
                repr(self.solidId)
            )
            
    def is_same_change_as(self, other):
        return self.solidId == other.solidId
        
        
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
        RemoveObject,
        ChangeObject,
        AddProperty,
        RemoveProperty,
        ChangeProperty,
        TieSolid,
        UntieSolid,
    )
    
    # We keep lists of each type of delta, so that we can merge or produce 
    # conflict warnings based on the existence of two compatible/incompatible 
    # deltas at once.
    deltaListsDict = {
        deltaType : []
        for deltaType in deltaTypes
    }
    
    def merge(delta):
        ''' Attempts to merge the given delta with an existing one.
        
        Fails silently if there are no candidates for merging.
        
        If a merge conflict is detected, emits a warning, marks the 
        conflicting deltas to be discarded, and raises DeltaMergeConflict.
        
        '''
        
        if isinstance(delta, ChangeObject):
            # Check for conflicts with RemoveObject deltas.
            for other in deltaListsDict[RemoveObject]:
                if delta.vmfClass == other.vmfClass and delta.id == other.id:
                    # Conflict!
                    print (
                        "CONFLICT WARNING: ChangeObject delta conflicts with "
                        "RemoveObject delta!"
                    )
                    print '\t', delta
                    print '\t', other
                    
                    # Discard only the RemoveObject delta, because it makes 
                    # human intervention a bit easier.
                    other.shouldDiscard = True
                    
                    raise DeltaMergeConflict
                    
            # Attempt to merge.
            for other in deltaListsDict[ChangeObject]:
                if delta.is_same_change_as(other):
                    # Merge is possible. Discard this delta.
                    delta.shouldDiscard = True
                    
        elif isinstance(delta, RemoveObject):
            # Check for other RemoveObject deltas that we can merge with.
            for other in deltaListsDict[RemoveObject]:
                if delta.is_same_change_as(other):
                    delta.shouldDiscard = True
                    
        elif isinstance(delta, AddProperty):
            for other in deltaListsDict[AddProperty]:
                if delta.is_same_change_as(other):
                    if other.value == delta.value:
                        # Can be merged.
                        delta.shouldDiscard = True
                        
                    else:
                        # Conflict!
                        print (
                            "CONFLICT WARNING: AddProperty conflict detected!"
                        )
                        print '\t', delta
                        print '\t', other
                        
                        # Discard both deltas.
                        delta.shouldDiscard = True
                        other.shouldDiscard = True
                        
                        raise DeltaMergeConflict
                        
        elif isinstance(delta, ChangeProperty):
            # Check for conflicts with RemoveProperty deltas.
            for other in deltaListsDict[RemoveProperty]:
                if (delta.vmfClass == other.vmfClass and 
                        delta.id == other.id and
                        delta.key == other.key):
                        
                    # Conflict!
                    print (
                        "CONFLICT WARNING: ChangeProperty delta conflicts "
                        "with RemoveProperty delta!"
                    )
                    print '\t', delta
                    print '\t', other
                    
                    # Discard only the RemoveProperty delta, because it makes 
                    # human intervention a bit easier.
                    other.shouldDiscard = True
                    
                    raise DeltaMergeConflict
                    
            # Attempt to merge.
            for other in deltaListsDict[ChangeProperty]:
                if delta.is_same_change_as(other):
                    if other.value == delta.value:
                        # Merge is possible. Discard this delta.
                        delta.shouldDiscard = True
                        
                    else:
                        # Conflict!
                        print (
                            "CONFLICT WARNING: ChangeProperty conflict "
                            "detected!"
                        )
                        print '\t', delta
                        print '\t', other
                        
                        # Discard both deltas.
                        delta.shouldDiscard = True
                        other.shouldDiscard = True
                        
                        raise DeltaMergeConflict
                        
        elif isinstance(delta, RemoveProperty):
            # Check for other RemoveProperty deltas that we can merge with.
            for other in deltaListsDict[RemoveProperty]:
                if delta.is_same_change_as(other):
                    delta.shouldDiscard = True
                    
        elif isinstance(delta, TieSolid):
            # Check for other TieSolid deltas that we can merge with.
            for other in deltaListsDict[TieSolid]:
                if delta.is_same_change_as(other):
                    if delta.entityId == other.entityId:
                        # Merge is possible. Discard this delta.
                        delta.shouldDiscard = True
                        
                    else:
                        # Conflict!
                        print (
                            "CONFLICT WARNING: TieSolid conflict detected!"
                        )
                        print '\t', delta
                        print '\t', other
                        
                        # Discard both deltas.
                        delta.shouldDiscard = True
                        other.shouldDiscard = True
                        
                        raise DeltaMergeConflict
                        
        elif isinstance(delta, UntieSolid):
            # Check for other UntieSolid deltas that we can merge with.
            for other in deltaListsDict[UntieSolid]:
                if delta.is_same_change_as(other):
                    delta.shouldDiscard = True
                    
    ##################
    # End of merge() #
    ##################
    
    conflict = False
    for deltaType in deltaTypes:
        for deltas in deltaLists:
            for delta in deltas:
                if isinstance(delta, deltaType):
                    # Attempt to merge, if possible.
                    try:
                        merge(delta)
                    except DeltaMergeConflict:
                        conflict = True
                        
                    # Add the delta to the dictionary regardless of whether or 
                    # not the merge was successful. If it was merged, it was 
                    # marked as discarded and will be cleaned up later on.
                    deltaListsDict[deltaType].append(delta)
                    
    # Now that we have all our deltas merged and ready, put them all into a 
    # single list of deltas to be returned.
    mergedDeltas = [
        delta
        for deltaType in deltaTypes
            for delta in deltaListsDict[deltaType]
                if not delta.shouldDiscard
    ]
    
    if conflict:
        raise DeltaMergeConflict(mergedDeltas)
    else:
        return mergedDeltas
        
        