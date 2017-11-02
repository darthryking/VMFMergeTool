"""

vmfmerge.py
By DKY

Version 0.0.0c DEV

VMF Merge Tool

"""

__version__ = '0.0.0c DEV'

import sys
from datetime import datetime
from argparse import ArgumentParser

from vmf import VMF, load_vmfs, get_parent, compare_vmfs
from vmfdelta import DeltaMergeConflict, merge_delta_lists


def parse_args():
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

    return parser.parse_args()
    
    
def main():
    args = parse_args()
    
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
    print "Loading VMFs..."
    vmfs = load_vmfs(vmfPaths)
    
    # Determine the parent VMF.
    if autoParent:
        parent = get_parent(vmfs)
    else:
        parent = vmfs[0]
        
    # Determine the child VMFs.
    children = [vmf for vmf in vmfs if vmf is not parent]
    
    # Generate lists of deltas for each child.
    print "Generating delta lists..."
    deltaLists = [compare_vmfs(parent, child) for child in children]
    
    if dumpIndividual:
        for deltas in deltaLists:
            print '\n'.join(repr(delta) for delta in deltas)
            print ""
            
        return 0
        
    # Merge the delta lists into a single list of deltas, to be applied on top 
    # of the parent.
    print "Merging deltas..."
    try:
        mergedDeltas = merge_delta_lists(deltaLists, aggressive=aggressive)
    except DeltaMergeConflict as e:
        print str(e)
        mergedDeltas = e.partialDeltas
        
    if dumpProposed:
        print '\n'.join(repr(delta) for delta in mergedDeltas)
        return 0
        
    # Apply the merged deltas to the parent.
    print "Applying deltas..."
    parent.apply_deltas(mergedDeltas)
    
    # Write the mutated parent to the target VMF path.
    print "Writing merged VMF..."
    parent.write_path('out.vmf')
    
    print "Done!"
    print "Total time: {}".format(datetime.now() - startTime)
    
    return 0
    
    
if __name__ == '__main__':
    sys.exit(main())
    