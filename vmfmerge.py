"""

vmfmerge.py
By DKY

Version 0.0.0 DEV

VMF Merge Tool

"""

__version__ = '0.0.0 DEV'

import sys
from argparse import ArgumentParser

from vmf import VMF, load_vmfs, get_parent, compare_vmfs
from vmfdelta import DeltaMergeConflict, merge_delta_lists

_parser = ArgumentParser(
        description="VMF Merge Tool",
    )
    
_parser.add_argument(
        '--version',
        action='version',
        version=__version__,
    )
    
_parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Noisily display progress messages throughout the procedure.',
    )
    
_parser.add_argument(
        'vmfs',
        nargs='+',
        metavar='vmf',
        help='The name of a *.vmf file, or the path to a *.vmf file.',
    )
    
_parser.add_argument(
        '-n', '--no-auto-parent',
        action='store_true',
        help=
            'Do not try to automatically figure out which VMF is the parent '
            'VMF. Instead, simply assume that the first VMF in the argument '
            'list is the parent. (Can be dangerous-- Use with care!)'
            ,
    )
    
_parser.add_argument(
        '-i', '--dump-individual',
        action='store_true',
        help=
            'Instead of merging, output a list of individual per-file deltas '
            'to stdout.'
            ,
    )
    
_parser.add_argument(
        '-p', '--dump-proposed',
        action='store_true',
        help=
            'Instead of merging, output a list of all proposed merge deltas '
            'to stdout.'
            ,
    )
    
_parser.add_argument(
        '-A', '--aggressive',
        action='store_true',
        help='Enable aggressive conflict resolution.',
    )
    
_args = _parser.parse_args()


def main():
    vmfPaths = _args.vmfs
    verbose = _args.verbose
    aggressive = _args.aggressive
    autoParent = not _args.no_auto_parent
    dumpIndividual = _args.dump_individual
    dumpProposed = _args.dump_proposed
    
    if dumpProposed and dumpIndividual:
        sys.stderr.write(
                "ERROR: --dump-individual and --dump-proposed are mutually "
                "exclusive!\n"
            )
        return 1
        
    # Load all VMFs.
    vmfs = load_vmfs(vmfPaths)
    
    # Determine the parent VMF.
    if autoParent:
        parent = get_parent(vmfs)
    else:
        parent = vmfs[0]
        
    # Determine the child VMFs.
    children = [vmf for vmf in vmfs if vmf is not parent]
    
    # Generate lists of deltas for each child.
    deltaLists = [compare_vmfs(parent, child) for child in children]
    
    if dumpIndividual:
        for deltas in deltaLists:
            print '\n'.join(repr(delta) for delta in deltas)
            print ""
            
        return 0
        
    # Merge the delta lists into a single list of deltas, to be applied on top 
    # of the parent.
    try:
        mergedDeltas = merge_delta_lists(deltaLists, aggressive=aggressive)
    except DeltaMergeConflict as e:
        print str(e)
        mergedDeltas = e.partialDeltas
        
    if dumpProposed:
        print '\n'.join(repr(delta) for delta in mergedDeltas)
        return 0
        
    # Apply the merged deltas to the parent.
    parent.apply_deltas(mergedDeltas)
    
    # Write the mutated parent to the target VMF path.
    parent.write_path('out.vmf')
    
    return 0
    
    
if __name__ == '__main__':
    sys.exit(main())
    