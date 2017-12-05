import unittest

import vmf
import vmfdelta

VMF = None


class TestMerge(unittest.TestCase):
    def setUp(self):
        global VMF
        VMF = vmf.VMF
        
    def test_merge_basic(self):
        deltas1 = [
            vmfdelta.AddObject(None, VMF.SOLID, 1),
        ]
        
        deltas2 = [
            vmfdelta.AddObject(None, VMF.SOLID, 2),
        ]
        
        expected = get_properties(
            [
                vmfdelta.AddObject(None, VMF.SOLID, 1),
                vmfdelta.AddObject(None, VMF.SOLID, 2),
            ]
        )
        
        actual = get_properties(
            vmfdelta.merge_delta_lists([deltas1, deltas2])
        )
        
        self.assertEqual(expected, actual)
        
    def test_merge_overlap(self):
        deltas1 = [
            vmfdelta.ChangeObject(VMF.SOLID, 1),
            vmfdelta.ChangeObject(VMF.SOLID, 2),
            vmfdelta.ChangeObject(VMF.SOLID, 3),
        ]
        
        deltas2 = [
            vmfdelta.ChangeObject(VMF.SOLID, 2),
            vmfdelta.ChangeObject(VMF.SOLID, 3),
            vmfdelta.ChangeObject(VMF.SOLID, 4),
        ]
        
        expected = get_properties(
            [
                vmfdelta.ChangeObject(VMF.SOLID, 1),
                vmfdelta.ChangeObject(VMF.SOLID, 2),
                vmfdelta.ChangeObject(VMF.SOLID, 3),
                vmfdelta.ChangeObject(VMF.SOLID, 4),
            ]
        )
        
        actual = get_properties(
            vmfdelta.merge_delta_lists([deltas1, deltas2])
        )
        
        self.assertEqual(expected, actual)
        
    def test_merge_overlap_3(self):
        deltas1 = [
            vmfdelta.ChangeObject(VMF.SOLID, 1),
            vmfdelta.ChangeObject(VMF.SOLID, 2),
            vmfdelta.ChangeObject(VMF.SOLID, 3),
        ]
        
        deltas2 = [
            vmfdelta.ChangeObject(VMF.SOLID, 2),
            vmfdelta.ChangeObject(VMF.SOLID, 3),
            vmfdelta.ChangeObject(VMF.SOLID, 4),
        ]
        
        deltas3 = [
            vmfdelta.ChangeObject(VMF.SOLID, 3),
            vmfdelta.ChangeObject(VMF.SOLID, 4),
            vmfdelta.ChangeObject(VMF.SOLID, 5),
        ]
        
        expected = get_properties(
            [
                vmfdelta.ChangeObject(VMF.SOLID, 1),
                vmfdelta.ChangeObject(VMF.SOLID, 2),
                vmfdelta.ChangeObject(VMF.SOLID, 3),
                vmfdelta.ChangeObject(VMF.SOLID, 4),
                vmfdelta.ChangeObject(VMF.SOLID, 5),
            ]
        )
        
        actual = get_properties(
            vmfdelta.merge_delta_lists([deltas1, deltas2, deltas3])
        )
        
        self.assertEqual(expected, actual)
        
    def test_merge_conflict(self):
        deltas1 = [
            vmfdelta.ChangeObject(VMF.SOLID, 1),
        ]
        
        deltas2 = [
            vmfdelta.RemoveObject(VMF.SOLID, 1),
        ]
        
        expected = get_properties(
            [
                vmfdelta.RemoveObject(VMF.SOLID, 1),
            ]
        )
        
        expectedConflicts = get_properties(
            [
                vmfdelta.ChangeObject(VMF.SOLID, 1),
            ]
        )
        
        try:
            vmfdelta.merge_delta_lists([deltas1, deltas2])
        except vmfdelta.DeltaMergeConflict as e:
            actual = get_properties(e.partialDeltas)
            conflicts = get_properties(e.conflictedDeltas)
        else:
            self.fail("Did not throw DeltaMergeConflict!")
            
        self.assertEqual(expected, actual)
        self.assertEqual(expectedConflicts, conflicts)
        
        
def get_properties(objects):
    return set(tuple(object.__dict__.items()) for object in objects)
    
    
if __name__ == '__main__':
    unittest.main()
    