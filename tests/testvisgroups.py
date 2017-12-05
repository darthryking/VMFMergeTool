import unittest

import vmf
import vmfdelta

VMF = None


class TestVisGroups(unittest.TestCase):
    def setUp(self):
        global VMF
        VMF = vmf.VMF
        
    def test_add_visgroup(self):
        parentVmf = VMF.from_path('test.vmf')
        childVmf = VMF.from_path('test_add_visgroup.vmf')
        
        deltas = vmf.compare_vmfs(parentVmf, childVmf)
        
        expected = set(
            get_properties(
                [
                    vmfdelta.AddObject(None, VMF.VISGROUP, 1),
                    vmfdelta.AddProperty(
                        VMF.VISGROUP, 1,
                        'name', "Test 1",
                    ),
                    vmfdelta.AddProperty(
                        VMF.VISGROUP, 1,
                        'color', '100 117 234',
                    ),
                    vmfdelta.AddToVisGroup(VMF.SOLID, 2, 1),
                ]
            )
        )
        actual = set(get_properties(deltas))
        
        self.assertEquals(expected, actual)
        
    def test_remove_visgroup(self):
        pass
        
    def test_move_visgroup(self):
        pass
        
    def test_add_to_visgroup(self):
        pass
        
    def test_remove_from_visgroup(self):
        pass
        
    def test_hide_object(self):
        pass
        
    def test_unhide_object(self):
        pass
        
        
def get_properties(objects):
    return tuple(tuple(object.__dict__.items()) for object in objects)
    
    
if __name__ == '__main__':
    unittest.main()
    