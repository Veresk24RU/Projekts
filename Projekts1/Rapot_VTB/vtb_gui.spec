# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.utils.hooks import collect_submodules

base_hiddenimports = [
    'sqlite3',
    'openpyxl',
    'pandas',
    'numpy',
    'requests',
    'reportlab',
]
base_hiddenimports += collect_submodules('reportlab')


a = Analysis(
    ['vtb_gui.py'],
    pathex=[],
    binaries=[],
    datas=[('ReadOT4ET.py', '.'), ('HTTP-Req_PORTFOLIO.py', '.'), ('ANALIZ_VTB.py', '.')],
    hiddenimports=base_hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='vtb_gui',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['avatar.ico'],
)
