"""

gui.py

All the logic required to run VMFMerge in GUI mode.

"""

import os
import sys
from io import StringIO
from queue import Queue
from threading import RLock
from functools import partial
from collections import deque
from concurrent.futures import ThreadPoolExecutor

from PySide2 import QtCore, QtUiTools, QtWidgets

import vmfmerge
from vmf import VMF, get_parent
from constants import GUI_FILE_DIR

_app = None
_executor = None
_vmfCache = None
_loadTaskFutureForPath = {}


class VMFCache:
    """ An expandable-size cache for VMFs. This lets us skip the load process
    for VMFs that we've already loaded before, which is helpful for VMFs that
    take a long time to parse.
    
    """
    
    def __init__(self):
        self.maxSize = 1
        self.data = {}
        self.unusedPaths = set()
        self.pendingUnusedPaths = set()
        
        self._mutex = RLock()
        
    def increase_max_size(self, maxSize):
        ''' Increases the max size of the cache to the given number.
        If the requested max size is less than the current size, this does
        nothing.
        
        '''
        
        with self._mutex:
            if maxSize > self.maxSize:
                self.set_max_size(maxSize)
                
    def set_max_size(self, maxSize):
        with self._mutex:
            if maxSize < self.get_vmf_count():
                raise ValueError("Can't clear enough unused entries!")
                
            self.evict_unused()
            self.maxSize = maxSize
            
            assert len(self.data) <= self.maxSize
            
    def add_vmf(self, vmf):
        vmfPath = vmf.path
        
        with self._mutex:
            assert len(self.data) <= self.maxSize
            
            if vmfPath in self.pendingUnusedPaths:
                # This VMF has been preemptively marked as unused.
                # Don't bother caching it.
                self.pendingUnusedPaths.remove(vmfPath)
                return
                
            if len(self.data) >= self.maxSize:
                if len(self.unusedPaths) > 0:
                    self.evict_unused(limit=1)
                else:
                    raise ValueError("VMF cache limit reached!")
                    
            self.data[vmfPath] = vmf
            
            assert len(self.data) <= self.maxSize
            
    def mark_used(self, *vmfPaths):
        with self._mutex:
            for vmfPath in vmfPaths:
                if vmfPath in self.unusedPaths:
                    self.unusedPaths.remove(vmfPath)
                    
    def mark_unused(self, *vmfPaths):
        with self._mutex:
            for vmfPath in vmfPaths:
                if vmfPath in self.data:
                    self.unusedPaths.add(vmfPath)
                else:
                    self.pendingUnusedPaths.add(vmfPath)
                    
    def evict_unused(self, limit=float('inf')):
        with self._mutex:
            for i, unusedPath in enumerate(set(self.unusedPaths)):
                if i >= limit:
                    break
                    
                del self.data[unusedPath]
                self.unusedPaths.remove(unusedPath)
                
                print("Evicted", unusedPath)
                
            assert len(self.data) <= self.maxSize
            
    def has_vmf_path(self, path):
        with self._mutex:
            return path in self.data
            
    def get_vmfs(self):
        with self._mutex:
            return [
                vmf for vmf in self.data.values()
                if vmf.path not in self.unusedPaths
            ]
            
    def get_vmf_count(self):
        with self._mutex:
            return len(self.data) - len(self.unusedPaths)
            
            
class VMFList(QtWidgets.QListWidget):
    LIST_MIMETYPE = 'application/x-qabstractitemmodeldatalist'
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.add_items_callback = lambda *x: None
        
    def set_add_items_callback(self, callback):
        self.add_items_callback = callback
        
    def dedupe(self, items):
        ''' Remove stuff we already have from the given list of items. '''
        
        for i in range(self.count()):
            item = self.item(i).text()
            if item in items:
                items.remove(item)
                
    def addItems(self, items):
        self.dedupe(items)
        super().addItems(items)
        self.add_items_callback(addedPaths=items)
        
    def insertItems(self, items):
        self.dedupe(items)
        super().insertItems(items)
        self.add_items_callback(addedPaths=items)
        
    def dragEnterEvent(self, e):
        mimeData = e.mimeData()
        
        if mimeData.hasUrls() or mimeData.hasFormat(self.LIST_MIMETYPE):
            e.accept()
        else:
            e.ignore()
            
    def dropEvent(self, e):
        mimeData = e.mimeData()
        
        if mimeData.hasUrls():
            filePaths = (url.toLocalFile() for url in e.mimeData().urls())
            vmfs = [
                filePath.replace('/', '\\')
                for filePath in filePaths
                    if filePath.endswith('.vmf')
            ]
            
            if vmfs:
                vmfs.sort()
                self.addItems(vmfs)
                e.accept()
                return
                
        elif mimeData.hasFormat(self.LIST_MIMETYPE):
            super().dropEvent(e)
            return
            
        e.ignore()
        
        
class BaseWindow(QtCore.QObject):
    def __init__(self, uiFileName):
        super().__init__()
        
        uiFilePath = os.path.join(GUI_FILE_DIR, uiFileName)
        
        self.loader = QtUiTools.QUiLoader()
        self.loader.registerCustomWidget(VMFList)
        self.loader.registerCustomWidget(LoadingDialog)
        
        self.window = self.load_ui(uiFilePath)
        self.window.installEventFilter(self)
        
        self._allowClose = True
        
    def load_ui(self, uiFileName):
        uiFile = QtCore.QFile(uiFileName)
        uiFile.open(QtCore.QFile.ReadOnly)
        
        return self.loader.load(uiFile)
        
    def find_child(self, *args, **kwargs):
        return self.window.findChild(*args, **kwargs)
        
    def allow_close(self):
        self._allowClose = True
        
        self.window.setWindowFlags(
            self.window.windowFlags() | QtCore.Qt.WindowCloseButtonHint
        )
        
    def disallow_close(self):
        self._allowClose = False
        
        self.window.setWindowFlags(
            self.window.windowFlags() & ~QtCore.Qt.WindowCloseButtonHint
        )
        
    def show(self):
        self.window.show()
        
    def hide(self):
        self.window.hide()
        
    def close(self):
        self.allow_close()
        self.window.close()
        
    def eventFilter(self, watched, event):
        if watched is self.window:
            if event.type() == QtCore.QEvent.Close and not self._allowClose:
                event.ignore()
                return True
                
            else:
                return self.handle_event(event)
                
        else:
            return False
            
    def handle_event(self, event):
        return False
        
        
class MainWindow(BaseWindow):
    UI_FILE_NAME = 'vmfmerge.ui'
    
    def __init__(self):
        super().__init__(self.UI_FILE_NAME)
        
        self.lbl_version = self.find_child(
            QtWidgets.QLabel,
            'lbl_version',
        )
        self.lbl_version.setText(
            "<h3>(Version {})</h3>".format(vmfmerge.__version__)
        )
        
        self.lbl_parentVMF = self.find_child(
            QtWidgets.QLabel,
            'lbl_parentVMF',
        )
        self._defaultParentLabelText = self.lbl_parentVMF.text()
        
        self.list_vmfs = self.find_child(
            QtWidgets.QListWidget,
            'list_vmfs',
        )
        self.list_vmfs.set_add_items_callback(self.list_updated)
        
        self.btn_addVMFs = self.find_child(
            QtWidgets.QPushButton,
            'btn_addVMFs',
        )
        self.btn_addVMFs.clicked.connect(self.add_vmfs)
        
        self.btn_removeVMFs = self.find_child(
            QtWidgets.QPushButton,
            'btn_removeVMFs',
        )
        self.btn_removeVMFs.clicked.connect(self.remove_vmfs)
        
        self.btn_merge = self.find_child(
            QtWidgets.QPushButton,
            'btn_merge',
        )
        self.btn_merge.clicked.connect(self.start_merge)
        
        self.loadingDialog = LoadingDialog(self)
        self.mergeWindow = MergeWindow(self)
        
        self.updateTimer = QtCore.QTimer()
        
        self._mergeInProgress = False
        
    def get_vmf_count(self):
        return self.list_vmfs.count()
        
    def add_vmfs(self):
        filePaths, filter = QtWidgets.QFileDialog.getOpenFileNames(
            caption="Browse for VMFs",
            filter="Valve Map Files (*.vmf)",
        )
        
        vmfs = sorted(
            filePath.replace('/', '\\')
            for filePath in filePaths
                if filePath.endswith('.vmf')
        )
        
        self.list_vmfs.addItems(vmfs)
        
    def remove_vmfs(self):
        removedVMFPaths = []
        for item in self.list_vmfs.selectedItems():
            self.list_vmfs.takeItem(self.list_vmfs.row(item))
            removedVMFPaths.append(item.text())
            
        self.list_updated(removedPaths=removedVMFPaths)
        
    def list_updated(self, addedPaths=[], removedPaths=[]):
        self.btn_removeVMFs.setEnabled(self.list_vmfs.count() > 0)
        
        if removedPaths:
            for path in removedPaths:
                try:
                    future = _loadTaskFutureForPath[path]
                except KeyError:
                    pass
                else:
                    future.cancel()
                    
            _vmfCache.mark_unused(*removedPaths)
            
            self.update_parent_label()
            
            if _vmfCache.get_vmf_count() == 0:
                self.btn_merge.setEnabled(False)
                
        if addedPaths:
            self.btn_merge.setEnabled(False)
            
            _vmfCache.increase_max_size(self.list_vmfs.count())
            
            for path in addedPaths:
                future = _executor.submit(load_vmf, path, self.loadingDialog)
                _loadTaskFutureForPath[path] = future
                
            self.loadingDialog.start_updating()
            self.loadingDialog.show()
            
    def finished_loading(self):
        assert _vmfCache.get_vmf_count() > 0
        
        self.update_parent_label()
        
        self.btn_merge.setEnabled(True)
        
    def update_parent_label(self):
        try:
            parentVMFPath = get_parent(_vmfCache.get_vmfs()).path
        except StopIteration:
            labelText = self._defaultParentLabelText
        else:
            labelText = os.path.basename(parentVMFPath)
            
        self.lbl_parentVMF.setText(labelText)
        
    def start_merge(self):
        self.btn_merge.setEnabled(False)
        self._mergeInProgress = True
        
        def do_merge():
            vmfs = _vmfCache.get_vmfs()
            parent = get_parent(vmfs)
            children = [vmf for vmf in vmfs if vmf is not parent]
            
            return vmfmerge.do_merge(
                parent, children,
                noParentSideEffects=True,
                update_callback=self.mergeWindow.update_merge_progress,
            )
            
        future = _executor.submit(do_merge)
        
        def check_result():
            if future.done():
                try:
                    conflictedDeltas = future.result()
                except Exception:
                    raise
                else:
                    if conflictedDeltas:
                        conflictedVMFs = [
                            os.path.basename(path) for path in sorted(
                                set(
                                    delta.originVMF.path
                                    for delta in conflictedDeltas
                                )
                            )
                        ]
                        
                        assert len(conflictedVMFs) > 1
                        
                        if len(conflictedVMFs) == 2:
                            conflictedVMFsStr = ' and '.join(conflictedVMFs)
                        else:
                            conflictedVMFsStr = ', '.join(conflictedVMFs[:-1])
                            conflictedVMFsStr += ', and ' + conflictedVMFs[-1]
                            
                        QtWidgets.QMessageBox.warning(
                            self.mergeWindow.window,
                            "Manual Merge Required",
                            "Note: Merge conflicts were detected between "
                            "{}.\n\n"
                            "Manual Merge VisGroups have been created. "
                            "You will need to use the Manual Merge VisGroups "
                            "in Hammer to complete the merge."
                                .format(conflictedVMFsStr),
                            buttons=QtWidgets.QMessageBox.Ok,
                        )
                finally:
                    self.updateTimer.stop()
                    self.updateTimer.timeout.disconnect()
                    
        self.updateTimer.setInterval(100)
        self.updateTimer.timeout.connect(check_result)
        self.updateTimer.start()
        
        self.mergeWindow.reset()
        self.mergeWindow.start_polling_for_updates()
        self.mergeWindow.show()
        
    def merge_complete(self):
        self.btn_merge.setEnabled(self.list_vmfs.count() > 0)
        self._mergeInProgress = False
        
    def handle_event(self, event):
        if event.type() == QtCore.QEvent.Close:
            if self._mergeInProgress:
                QtWidgets.QMessageBox.warning(
                    self.window,
                    "Merge in Progress",
                    "A merge is currently in progress.\n"
                    "Please wait for the merge to complete before "
                    "closing VMFMerge.",
                    buttons=QtWidgets.QMessageBox.Ok,
                )
                
                event.ignore()
                
                return True
                
            else:
                self.loadingDialog.close()
                self.mergeWindow.close()
                
        return False
        
        
class LoadingDialog(BaseWindow):
    UI_FILE_NAME = 'loadvmfsdialog.ui'
    UPDATE_INTERVAL_MS = 100
    
    def __init__(self, mainWindow):
        super().__init__(self.UI_FILE_NAME)
        
        self.disallow_close()
        
        self.lbl_text = self.find_child(QtWidgets.QLabel, 'lbl_text')
        
        self.progressBar = self.find_child(
            QtWidgets.QProgressBar,
            'progressBar',
        )
        
        self._mainWindowRef = mainWindow
        self.updateTimer = QtCore.QTimer()
        self.currentVMF = None
        
    def set_current_vmf(self, currentVMF):
        self.currentVMF = currentVMF
        
    def start_updating(self):
        self.update()
        
        self.updateTimer.setInterval(self.UPDATE_INTERVAL_MS)
        self.updateTimer.timeout.connect(self.update)
        self.updateTimer.start()
        
    def stop_updating(self):
        self.updateTimer.stop()
        self.updateTimer.timeout.disconnect()
        
    def update(self):
        numVMFsToLoad = 0
        numLoadedVMFs = 0
        
        for future in _loadTaskFutureForPath.values():
            if not future.cancelled():
                numVMFsToLoad += 1
                
                if future.done():
                    e = future.exception()
                    if e is not None:
                        raise e
                        
                    numLoadedVMFs += 1
                    
        if self.currentVMF is None:
            self.lbl_text.setText(
                "Loading... ({}/{})".format(
                    min(numLoadedVMFs + 1, numVMFsToLoad),
                    numVMFsToLoad
                )
            )
        else:
            self.lbl_text.setText(
                "Loading {}... ({}/{})".format(
                    self.currentVMF,
                    min(numLoadedVMFs + 1, numVMFsToLoad),
                    numVMFsToLoad,
                )
            )
            
        self.progressBar.setMaximum(numVMFsToLoad)
        self.progressBar.setValue(numLoadedVMFs)
        
        if numLoadedVMFs == numVMFsToLoad:
            _loadTaskFutureForPath.clear()
            
            self.currentVMF = None
            self._mainWindowRef.finished_loading()
            
            self.stop_updating()
            self.close()
            
            
class MergeWindow(BaseWindow):
    UI_FILE_NAME = 'mergewindow.ui'
    UPDATE_POLL_INTERVAL_MS = 100
    
    def __init__(self, mainWindow):
        super().__init__(self.UI_FILE_NAME)
        
        self.lbl_progress = self.find_child(
            QtWidgets.QLabel,
            'lbl_progress',
        )
        
        self.progressBar = self.find_child(
            QtWidgets.QProgressBar,
            'progressBar',
        )
        
        self.txt_log = self.find_child(
            QtWidgets.QTextEdit,
            'txt_log',
        )
        
        self._mainWindowRef = mainWindow
        self.updateTimer = QtCore.QTimer()
        self._updateQ = Queue()
        
        self.progressTextBuffer = StringIO()
        
    def reset(self):
        self.disallow_close()
        
        self.lbl_progress.setText("Starting merge...")
        
        self.progressBar.setMaximum(1)
        self.progressBar.setValue(0)
        
        self.txt_log.clear()
        
        self.progressTextBuffer.seek(0)
        self.progressTextBuffer.truncate()
        
    def start_polling_for_updates(self):
        self.updateTimer.setInterval(self.UPDATE_POLL_INTERVAL_MS)
        self.updateTimer.timeout.connect(self._poll_updates)
        self.updateTimer.start()
        
    def stop_polling_for_updates(self):
        self.updateTimer.stop()
        self.updateTimer.timeout.disconnect()
        
    def _poll_updates(self):
        while not self._updateQ.empty():
            do_update = self._updateQ.get()
            do_update()
            
    def update_merge_progress(self, message, progress, maxProgress):
        def do_update():
            if message == "Done!":
                self.lbl_progress.setText(message)
                self.stop_polling_for_updates()
                
                self.allow_close()
                self.show()
                
                self._mainWindowRef.merge_complete()
                
            else:
                self.lbl_progress.setText(
                    "{} (Step {}/{})".format(
                        message,
                        progress + 1, maxProgress,
                    )
                )
                
            self.progressBar.setMaximum(maxProgress)
            self.progressBar.setValue(progress)
            
            self.progressTextBuffer.write(message + '\n')
            
            self.txt_log.setText(self.progressTextBuffer.getvalue())
            
        # Schedule our update to run on the main UI thread...
        self._updateQ.put(do_update)
        
        
def load_vmf(vmfPath, loadingDialog):
    print("Loading {}...".format(vmfPath))
    
    loadingDialog.set_current_vmf(os.path.basename(vmfPath))
    
    if not _vmfCache.has_vmf_path(vmfPath):
        vmf = VMF.from_path(vmfPath)
        
        print("Done loading {}".format(vmfPath))
        
        _vmfCache.add_vmf(vmf)
        
        print("Added {} to cache".format(vmfPath))
        
    else:
        print("Cache hit!")
        _vmfCache.mark_used(vmfPath)
        
        
def main(argv):
    QtWidgets.QApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling)
    
    global _app
    _app = QtWidgets.QApplication(argv)
    
    global _executor
    _executor = ThreadPoolExecutor(max_workers=1)
    
    global _vmfCache
    _vmfCache = VMFCache()
    
    with _executor:
        mainWindow = MainWindow()
        mainWindow.show()
        
        return _app.exec_()
        
        
if __name__ == '__main__':
    sys.exit(main(sys.argv))
    