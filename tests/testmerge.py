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
        
        with self.assertRaises(vmfdelta.DeltaMergeConflict) as contextManager:
            vmfdelta.merge_delta_lists([deltas1, deltas2])
            
        exception = contextManager.exception
        actual = get_properties(exception.partialDeltas)
        conflicts = get_properties(exception.conflictedDeltas)
        
        self.assertEqual(expected, actual)
        self.assertEqual(expectedConflicts, conflicts)
        
    def test_merge_conflict_3(self):
        deltas1 = [
            vmfdelta.ChangeObject(VMF.SOLID, 1),
            vmfdelta.AddProperty(VMF.SOLID, 1, 'key', 'value1'),
        ]
        
        deltas2 = [
            vmfdelta.ChangeObject(VMF.SOLID, 1),
            vmfdelta.AddProperty(VMF.SOLID, 1, 'key', 'value2'),
        ]
        
        deltas3 = [
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
                vmfdelta.AddProperty(VMF.SOLID, 1, 'key', 'value1'),
                vmfdelta.AddProperty(VMF.SOLID, 1, 'key', 'value2'),
            ]
        )
        
        with self.assertRaises(vmfdelta.DeltaMergeConflict) as contextManager:
            vmfdelta.merge_delta_lists([deltas1, deltas2, deltas3])
            
        exception = contextManager.exception
        actual = get_properties(exception.partialDeltas)
        conflicts = get_properties(exception.conflictedDeltas)
        
        self.assertEqual(expected, actual)
        self.assertEqual(expectedConflicts, conflicts)
        
    def test_merge_outputs_1(self):
        deltas1 = [
            vmfdelta.AddOutput(42, 'OnPressed', 'value1', 0),
        ]
        deltas2 = [
            vmfdelta.AddOutput(42, 'OnPressed', 'value2', 0),
        ]
        
        expected = get_properties(
            [
                vmfdelta.AddOutput(42, 'OnPressed', 'value1', 0),
                vmfdelta.AddOutput(42, 'OnPressed', 'value2', 0),
            ]
        )
        actual = get_properties(
            vmfdelta.merge_delta_lists([deltas1, deltas2])
        )
        
        self.assertEqual(expected, actual)
        
        
def get_properties(objects):
    return set(tuple(object.__dict__.items()) for object in objects)
    
    
if __name__ == '__main__':
    unittest.main()
    