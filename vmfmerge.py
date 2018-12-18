"""

vmfmerge.py
By DKY

Version 0.0.0f DEV

VMF Merge Tool

"""

__version__ = '0.0.0f DEV'

import os
import sys
import copy
import shutil
from datetime import datetime
from argparse import ArgumentParser
from collections import OrderedDict

from vmf import VMF, InvalidVMF, load_vmfs, get_parent, compare_vmfs
from vmfdelta import (
    DeltaMergeConflict,
    merge_delta_lists, create_conflict_resolution_deltas,
)


def parse_args(argv):
    parser = ArgumentParser(
        description="VMF Merge Tool",
    )

    parser.add_argument(
        '--version',
        action='version',
        version=__version__,
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help="Noisily display progress messages throughout the procedure.",
    )

    parser.add_argument(
        'vmfs',
        nargs='+',
        metavar='vmf',
        help="The name of a *.vmf file, or the path to a *.vmf file.",
    )

    parser.add_argument(
        '-n', '--no-auto-parent',
        action='store_true',
        help=
            "Do not try to automatically figure out which VMF is the parent "
            "VMF. Instead, simply assume that the first VMF in the argument "
            "list is the parent. (Can be dangerous-- Use with care!)"
            ,
    )

    parser.add_argument(
        '-i', '--dump-individual',
        action='store_true',
        help=
            "Instead of merging, output a list of individual per-file deltas "
            "to stdout."
            ,
    )

    parser.add_argument(
        '-p', '--dump-proposed',
        action='store_true',
        help=
            "Instead of merging, output a list of all proposed merge deltas "
            "to stdout."
            ,
    )

    parser.add_argument(
        '-A', '--aggressive',
        action='store_true',
        help="Enable aggressive conflict resolution.",
    )

    return parser.parse_args(argv)
    
    
def do_merge(
        parent, children,
        dumpIndividual=False, dumpProposed=False, aggressive=False,
        noParentSideEffects=False,
        update_callback=lambda *x: None
        ):
    """ Performs a merge of the given children's deltas into the parent.
    
    If `dumpIndividual` is True, this prints and returns the individual deltas
    for each child (in `{child : deltaList}` form), and does nothing else.
    
    If `dumpProposed` is True, this prints and returns a list of the proposed
    merged deltas, and does nothing else.
    
    The `aggressive` flag is not currently implemented.
    
    If `noParentSideEffects` is True, this leaves the given parent VMF
    untouched, modifying a deep copy instead of the original.
    
    The `update_callback` will be called at each stage of the process with the
    following arguments:
    
        update_callback(message, progress, maxProgress)
        
    ... where `message` is a human-readable message describing the current
    stage, `progress` is a 1-indexed integer representing the current stage
    number, and `maxProgress` is the number of required stages in this merge.
    
    If both `dumpIndividual` and `dumpProposed` are False, this function
    returns a list of deltas that were found to conflict during the merge.
    If there were no conflicted deltas found during the process, an empty list
    is returned.
    
    """
    
    class ProgressTracker:
        NUM_MERGE_STEPS = 4
        
        def __init__(self, children):
            self.progress = 0
            self.maxProgress = len(children) + self.NUM_MERGE_STEPS
            
        def update(self, message, increment=True):
            print(message)
            
            update_callback(
                message,
                min(self.progress, self.maxProgress),
                self.maxProgress,
            )
            
            if increment:
                self.progress += 1
                
    progressTracker = ProgressTracker(children)
    
    # Save a backup of the original parent.
    parentDir = os.path.dirname(parent.path)
    parentFileName = os.path.basename(parent.path)
    progressTracker.update("Creating a backup of {}...".format(parentFileName))
    
    parentName, ext = os.path.splitext(parentFileName)
    backupFileName = parentName + '_old' + ext
    backupFilePath = os.path.join(parentDir, backupFileName)
    
    shutil.copy(parent.path, backupFilePath)
    
    # If we don't want side-effects on the parent VMF, we should deep-copy it.
    if noParentSideEffects:
        parent = copy.deepcopy(parent)
        
    # Generate lists of deltas for each child.
    deltaListForChild = OrderedDict()
    for i, child in enumerate(children):
        progressTracker.update(
            "Generating delta list for {}...".format(
                os.path.basename(child.path)
            )
        )
        deltas = compare_vmfs(parent, child)
        deltaListForChild[child] = deltas
    
    if dumpIndividual:
        for child, deltas in deltaListForChild.items():
            print("Deltas for {}:".format(child.path))
            print('\n'.join(repr(delta) for delta in deltas))
            print("")
            
        return deltaListForChild
        
    # Fix up all deltas so that they have references to their origin VMF.
    for child, deltas in deltaListForChild.items():
        for delta in deltas:
            delta.originVMF = child
            
    # Merge the delta lists into a single list of deltas, to be applied on top 
    # of the parent.
    progressTracker.update("Merging deltas...")
    
    deltaLists = list(deltaListForChild.values())
    
    try:
        mergedDeltas = merge_delta_lists(deltaLists, aggressive=aggressive)
        
    except DeltaMergeConflict as e:
        print(str(e))
        mergedDeltas = e.partialDeltas
        
        print("")
        print("Conflicted deltas:")
        conflictedDeltas = e.conflictedDeltas
        for delta in conflictedDeltas:
            print("From {}:".format(delta.get_origin_filename()), repr(delta))
            
        print("")
        
        progressTracker.update("Creating Manual Merge VisGroups...")
        
        conflictResolutionDeltas = create_conflict_resolution_deltas(
            parent, conflictedDeltas
        )
        
        # print ""
        # print "Conflict resolution deltas:"
        # print '\n'.join(repr(delta) for delta in conflictResolutionDeltas)
        # print ""
        
        mergedDeltas += conflictResolutionDeltas
        
    else:
        conflictedDeltas = []
        
    if dumpProposed:
        print("Merged deltas:")
        print('\n'.join(repr(delta) for delta in mergedDeltas))
        return mergedDeltas
        
    # Apply the merged deltas to the parent.
    progressTracker.update("Applying deltas...")
    parent.apply_deltas(mergedDeltas)
    
    # Write the mutated parent to the target VMF path.
    progressTracker.update("Writing merged VMF...")
    # parent.write_path(parent.path)
    parent.write_path('out.vmf')
    
    # Done!
    progressTracker.update("Done!", increment=False)
    
    return conflictedDeltas
    
    
def main(argv):
    args = parse_args(argv[1:])
    
    vmfPaths = args.vmfs
    verbose = args.verbose
    aggressive = args.aggressive
    autoParent = not args.no_auto_parent
    dumpIndividual = args.dump_individual
    dumpProposed = args.dump_proposed
    
    if dumpProposed and dumpIndividual:
        sys.stderr.write(
            "ERROR: --dump-individual and --dump-proposed are mutually "
            "exclusive!\n"
        )
        return 1
        
    startTime = datetime.now()
    
    # Load all VMFs.
    print("Loading VMFs...")
    try:
        vmfs = load_vmfs(vmfPaths)
    except InvalidVMF as e:
        sys.stderr.write(
            "ERROR: {} is invalid: {}\n".format(e.path, e.message)
        )
        return 1
        
    # Determine the parent VMF.
    if autoParent:
        parent = get_parent(vmfs)
    else:
        parent = vmfs[0]
        
    # Determine the child VMFs.
    children = [vmf for vmf in vmfs if vmf is not parent]
    
    # Go!
    do_merge(parent, children, dumpIndividual, dumpProposed, aggressive)
    
    print("Total time: {}".format(datetime.now() - startTime))
    
    return 0
    
    
if __name__ == '__main__':
    sys.exit(main(sys.argv))
    