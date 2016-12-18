import unittest
import sys
import os

# Modify PYTHON_PATH so that we can import our modules for testing
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

import local

class TestBasicLocal(unittest.TestCase):
    def test_filehandle(self):
        """
        Test local file pipeline creation
        """
        testfname = "testfile"
        testfh = open(testfname, 'w')
        testcontents = "gorb" * 1000
        testfh.write(testcontents)
        testfh.close()
        
        pipeline_item = {"name": "test_file",
                         "type": local.PLI_FILE_TYPE,
                         "local_filepath": "./testfile"}
        fh = local.file_handle(pipeline_item, {})

        contents = fh.read()
        self.assertEqual(testcontents, contents)

    def test_versions(self):
        """
        Test that when we create a new version of a pipeline item,
        that item becomes the default and has a new hash
        """
        self.assertTrue(False)

    def test_pruning(self):
        """
        Test that files get pruned after a certain number of versions,
        according to the given policy
        """
        self.assertTrue(False)
        

    def test_isupper(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())


if __name__ == '__main__':
    unittest.main()
