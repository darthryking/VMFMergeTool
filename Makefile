.PHONY: all gui gui-debug cli clean

all: gui cli

gui:
	pyinstaller gui.py \
		--name vmfmerge-gui \
		--noconsole \
		--onefile \
		--hidden-import 'PySide2.QtXml' \
		--upx-exclude vcruntime140.dll \
		--upx-exclude msvcp140.dll \
		--upx-exclude qwindows.dll \
		--upx-exclude qwindowsvistastyle.dll \
		--add-data './vmfmerge.ui:.' \
		--add-data './loadvmfsdialog.ui:.' \
		--add-data './mergewindow.ui:.' \

gui-debug:
	pyinstaller gui.py \
		--name vmfmerge-gui \
		--onefile \
		--hidden-import 'PySide2.QtXml' \
		--upx-exclude vcruntime140.dll \
		--upx-exclude msvcp140.dll \
		--upx-exclude qwindows.dll \
		--upx-exclude qwindowsvistastyle.dll \
		--add-data './vmfmerge.ui:.' \
		--add-data './loadvmfsdialog.ui:.' \
		--add-data './mergewindow.ui:.' \

cli:
	pyinstaller vmfmerge.py \
		--name vmfmerge-cli \
		--onefile \
		--upx-exclude vcruntime140.dll \
		--upx-exclude msvcp140.dll \

clean:
	-rm -rf build/*
	-rm -rf build
	-rm -rf dist/*
	-rm -rf dist
