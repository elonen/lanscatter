import wx
import wx.adv

import appdirs

import sys, os, io, threading, traceback, json, platform
import PIL.Image, PIL.ImageOps, PIL.ImageColor
from contextlib import suppress
import multiprocessing
from datetime import datetime, timedelta
from pathlib import Path

from . import common, masternode, peernode

SETTINGS_DEFAULTS = {
    'local_peer_port': common.Defaults.TCP_PORT_PEER,
    'local_master_port': common.Defaults.TCP_PORT_MASTER,

    'remote_address': 'lanscatter-master',
    'remote_port': common.Defaults.TCP_PORT_MASTER,

    'sync_dir': ('C:\\' if any(platform.win32_ver()) else '~/') + 'lanscatter-sync-dir',
    'concurrent_transfers': common.Defaults.CONCURRENT_TRANSFERS_PEER,
    'upload_bandwidth': common.Defaults.BANDWIDTH_LIMIT_MBITS_PER_SEC,
    'rescan_interval': common.Defaults.DIR_SCAN_INTERVAL_PEER,

    'is_master': False,
    'autostart': False
}

def get_config_path(filename, create=False):
    base = Path(appdirs.user_config_dir(common.Defaults.APP_NAME))
    if create and not base.exists():
        base.mkdir(exist_ok=True, parents=True)
    return str(base / filename)

# To be run in separate process:
# run syncer and send stdout + exceptions through a pipe.
def sync_proc(conn, is_master, argv):
    try:
        class PipeOut(io.RawIOBase):
            def write(self, b):
                conn.send(b)
        out = PipeOut()
        sys.stdout, sys.stderr = out, out
        sys.argv = argv
        if is_master:
            masternode.main()
        else:
            peernode.main()
        conn.send((None, None))
    except Exception as e:
        conn.send((e, traceback.format_exc()))


# Settings dialog window -- instantiated on menu click
class SettingsDlg(wx.Dialog):
    def __init__(self, parent, title, settings):
        super(SettingsDlg, self).__init__(parent, title=title, style=wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER)
        self.settings = settings
        self.init_ui()
        self.Centre()

    def init_ui(self):
        panel = wx.Panel(self)
        vbox = wx.BoxSizer(wx.VERTICAL)

        st_basic = wx.SizerFlags().Border(direction=wx.UP | wx.LEFT | wx.RIGHT)
        st_expand = wx.SizerFlags().Expand().Border(direction=wx.UP | wx.LEFT | wx.RIGHT)
        st_expand_last = wx.SizerFlags().Expand().Border(direction=wx.UP | wx.DOWN | wx.LEFT | wx.RIGHT)
        st_prop = wx.SizerFlags().Proportion(1).Border(direction=wx.UP | wx.LEFT | wx.RIGHT)

        # Mode selector
        hb = wx.BoxSizer(wx.HORIZONTAL)
        vbox.Add(hb, st_basic)
        if True:
            # Radio button: client or server
            self.is_slave = wx.RadioButton(panel, label="Peer mode", style=wx.RB_GROUP)
            self.Bind(wx.EVT_RADIOBUTTON, self.on_radio_button, self.is_slave)
            self.is_slave.SetValue(True)
            hb.Add(self.is_slave, st_basic)

            self.is_master = wx.RadioButton(panel, label="Master mode")
            self.Bind(wx.EVT_RADIOBUTTON, self.on_radio_button, self.is_master)
            hb.Add(self.is_master, st_basic)

        # Sync dir selector
        hb = wx.BoxSizer(wx.HORIZONTAL)
        vbox.Add(hb, st_expand)
        if True:
            hb.Add(wx.StaticText(panel, label='Sync dir'), st_basic)
            self.sync_dir = wx.TextCtrl(panel)
            hb.Add(self.sync_dir, st_prop)
            btn = wx.Button(panel, label='Browse')
            btn.Bind(wx.EVT_BUTTON, self.on_pick_dir)
            hb.Add(btn, st_basic)

        peer_box = wx.StaticBox(panel, label="")
        peer_sizer = wx.StaticBoxSizer(peer_box, wx.VERTICAL)
        vbox.Add(peer_sizer, st_expand)
        if True:
            hb = wx.BoxSizer(wx.HORIZONTAL)
            peer_sizer.Add(hb, st_expand)
            if True:
                hb.Add(wx.StaticText(panel, label='Server address'), st_basic)
                self.remote_address = wx.TextCtrl(panel)
                hb.Add(self.remote_address, st_prop)

                hb.Add(wx.StaticText(panel, label='port'), st_basic)
                self.remote_port = wx.SpinCtrl(panel, value='1', min=1, max=65535, initial=common.Defaults.TCP_PORT_MASTER)
                hb.Add(self.remote_port, st_basic)

            hb = wx.BoxSizer(wx.HORIZONTAL)
            peer_sizer.Add(hb, st_expand_last)
            if True:
                hb.Add(wx.StaticText(panel, label='Local port (peer)'), st_basic)
                self.local_peer_port = wx.SpinCtrl(panel, value='1', min=1, max=65535, initial=common.Defaults.TCP_PORT_PEER)
                hb.Add(self.local_peer_port, st_basic)

        master_box = wx.StaticBox(panel, label="")
        master_sizer = wx.StaticBoxSizer(master_box, wx.VERTICAL)
        vbox.Add(master_sizer, st_expand)
        if True:
            hb = wx.BoxSizer(wx.HORIZONTAL)
            master_sizer.Add(hb, st_expand_last)
            if True:
                hb.Add(wx.StaticText(panel, label='Local port (master)'), st_basic)
                self.local_master_port = wx.SpinCtrl(panel, value='1', min=1, max=65535, initial=common.Defaults.TCP_PORT_MASTER)
                hb.Add(self.local_master_port, st_basic)

        adv_box = wx.StaticBox(panel, label="Advanced")
        adv_sizer = wx.StaticBoxSizer(adv_box, wx.VERTICAL)
        gs = wx.GridSizer(4, 2, wx.SizerFlags().GetDefaultBorder(), wx.SizerFlags().GetDefaultBorder())
        adv_sizer.Add(gs, st_basic)
        vbox.Add(adv_sizer, st_expand)
        if True:
            # Max concurrent transfers
            gs.Add(wx.StaticText(panel, label='Concurrent transfers'), st_basic)
            self.concurrent_transfers = wx.SpinCtrl(
                panel, value='2', min=1, max=1000, initial=common.Defaults.CONCURRENT_TRANSFERS_PEER)
            gs.Add(self.concurrent_transfers, st_basic)

            # Max concurrent transfers
            gs.Add(wx.StaticText(panel, label='Upload rate (Mbit/s)'), st_basic)
            self.upload_bandwidth = wx.SpinCtrl(
                panel, value='1000', min=1, max=100000, initial=common.Defaults.BANDWIDTH_LIMIT_MBITS_PER_SEC)
            gs.Add(self.upload_bandwidth, st_basic)

            # Dir scan interval
            gs.Add(wx.StaticText(panel, label='File rescan frequency (sec)'), st_basic)
            self.rescan_interval = wx.SpinCtrl(
                panel, value='120', min=5, max=60*60*24, initial=common.Defaults.DIR_SCAN_INTERVAL_PEER)
            gs.Add(self.rescan_interval, st_basic)

            # Autotart
            gs.Add(wx.StaticText(panel, label='Launch sync when app starts'), st_basic)
            self.autostart = wx.CheckBox(panel, label='Autostart')
            gs.Add(self.autostart, st_basic)

        vbox.AddSpacer(wx.SizerFlags().GetDefaultBorder()*3)
        vbox.AddStretchSpacer()

        # OK / Cancel
        hb = wx.BoxSizer(wx.HORIZONTAL)
        vbox.Add(hb, wx.SizerFlags().Align(wx.ALIGN_RIGHT).Right().Border())
        if True:
            hb.Add(wx.Button(panel, wx.ID_OK, label="OK"))
            hb.Add(wx.Button(panel, wx.ID_CANCEL, label="Cancel"))

        # Resize to fit
        vbox.AddSpacer(wx.SizerFlags().GetDefaultBorder()*6)
        panel.SetSizer(vbox)
        sz = vbox.GetMinSize()
        self.SetMinSize(wx.Size(int(sz.x), int(sz.y)+wx.SizerFlags().GetDefaultBorder()*2))
        self.Fit()

        # Set widget values from settings
        for key, default in SETTINGS_DEFAULTS.items():
            assert hasattr(self, key), f'No corresponding widget for settings "{key}".'
            getattr(self, key).SetValue(self.settings.get(key, default))
        self.on_radio_button(None)  # Update widget enable/disable


    def on_pick_dir(self, e):
        dlg = wx.DirDialog(self, "Choose a sync dir", defaultPath=self.sync_dir.GetValue(), style=wx.DD_DIR_MUST_EXIST)
        if dlg.ShowModal() == wx.ID_OK:
            self.sync_dir.SetValue(dlg.GetPath())

    def on_radio_button(self, event):
        self.remote_address.Enable(self.is_slave.GetValue())
        self.remote_port.Enable(self.is_slave.GetValue())
        self.local_peer_port.Enable(self.is_slave.GetValue())
        self.local_master_port.Enable(not self.is_slave.GetValue())

    def get_settings(self):
        res = SETTINGS_DEFAULTS.copy()
        for key, _ in SETTINGS_DEFAULTS.items():
            res[key] = getattr(self, key).GetValue()
        return res


# Simple log viewer window
class LogWindow(wx.Dialog):
    def __init__(self, parent=None, systray_icon: 'TaskBarIcon' = None):
        super(LogWindow, self).__init__(parent, style=wx.DEFAULT_DIALOG_STYLE | wx.RESIZE_BORDER)
        self.systray_icon = systray_icon
        panel = wx.Panel(self, wx.ID_ANY)
        self.Bind(wx.EVT_CLOSE, self.OnClose)

        self.log_widget = wx.TextCtrl(panel, wx.ID_ANY, size=(640, 480), style=wx.TE_MULTILINE | wx.TE_READONLY | wx.HSCROLL)
        self.log_widget.SetFont(wx.Font(10, wx.MODERN, wx.NORMAL, wx.NORMAL, False, u'Courier'))
        self.log_widget.WriteText(self.systray_icon.log_text.getvalue())

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(self.log_widget, 1, wx.ALL | wx.EXPAND, 5)
        panel.SetSizer(sizer)

        sz_x, sz_y = wx.DisplaySize()
        self.SetMinSize(wx.Size(int(sz_x/2), int(sz_y/2)))
        self.Fit()
        self.Centre()

    def OnClose(self, event):
        self.Destroy()
        self.systray_icon.log_win = None

    def write(self, txt):
        wx.CallAfter(self.log_widget.WriteText, txt)


# Animated sys tray icon with popup menu (main UI class for this app)
class TaskBarIcon(wx.adv.TaskBarIcon):

    def __init__(self, frame):
        super(wx.adv.TaskBarIcon, self).__init__()

        self.MENUID_STATUS_TEXT = None

        self.frame = frame
        self.toggle = 0
        self.icon_idx = 0
        wx.adv.TaskBarIcon.__init__(self)

        self.cur_progress_text = ''
        self.cur_status_text = '(not running)'
        self.syncer = None
        self.menu = None

        self.log_text = io.StringIO()
        self.log_win = None
        self.log_formatter = common.make_human_cli_status_func(print_func=lambda txt: self.write_log(txt + '\n'))

        self.latest_progress_change = datetime.utcnow() - timedelta(seconds=60)

        def resource_path(relative_path):
            """ Get absolute path to resource, works for dev and for PyInstaller """
            try:
                # PyInstaller creates a temp folder and stores path in _MEIPASS
                base_path = sys._MEIPASS
            except Exception:
                base_path = os.path.abspath(".")
            return os.path.join(base_path, relative_path)

        self.icons = []
        self.make_animated_icon(bgr=resource_path('gfx/icon__bgr.png'), wheel=resource_path('gfx/icon__wheel.png'))
        self.SetIcon(self.icon_inactive)

        # Read config file or use defaults
        self.settings = SETTINGS_DEFAULTS.copy()
        try:
            with open(get_config_path('gui.cfg'), 'r') as f:
                new_settings = json.loads(f.read())
                for key, default in SETTINGS_DEFAULTS.items():
                    self.settings[key] = new_settings.get(key, default)
        except FileNotFoundError:
            pass
        except IOError as e:
            print(f"Failed to load config file: {str(e)}")

        if self.settings['autostart']:
            self.on_menu_start_stop(None)

        # Start icon animator
        self.timer = wx.Timer(self)
        self.Bind(wx.EVT_TIMER, self.on_timer_tick)
        self.timer.Start(100)

        with suppress(AttributeError):  # Implemented (and needed) only on Windows
            wx.adv.NotificationMessage.UseTaskBarIcon(self)

    # Make progress animation icons by rotating given bitmap 360 degrees
    def make_animated_icon(self, bgr, wheel):
        dst_size = (16,16) if any(platform.win32_ver()) else (32,32)  # systray icon is 16px on Win, 32 on OSX
        wheel = PIL.Image.open(wheel)

        def pil_to_icon(image):
            return wx.Icon(wx.Bitmap.FromBufferRGBA(image.size[0], image.size[1], image.convert('RGBA').tobytes()))

        def tint(src, color):
            r, g, b, alpha = src.split()
            res = PIL.ImageOps.colorize(PIL.ImageOps.grayscale(src), (0,0,0,0), color)
            res.putalpha(alpha)
            return res

        bgr = tint(PIL.Image.open(bgr).resize(dst_size, resample=PIL.Image.BICUBIC), "#D1A000")

        for r in range(0, 64):
            angle = -360 * (r/64.0)  # 6.28319 = 360 dg in radians
            img = wheel.rotate(angle, resample=PIL.Image.BICUBIC).resize(dst_size, resample=PIL.Image.BICUBIC)
            img = tint(img, "#FFC300")
            self.icons.append(pil_to_icon(PIL.Image.alpha_composite(img, bgr)))

        bmp = wx.Bitmap()
        bmp.CopyFromIcon(self.icons[0])
        self.icon_inactive = wx.Icon(wx.Bitmap(bmp.ConvertToImage().ConvertToGreyscale()))

    # Overrides TaskBarIcon
    def CreatePopupMenu(self):
        menu = wx.Menu()

        menu_status_item = wx.MenuItem(menu, -1, self.cur_progress_text + self.cur_status_text)
        self.MENUID_STATUS_TEXT = menu_status_item.GetId()
        menu_status_item.Enable(False)
        menu.Append(menu_status_item)

        menu.AppendSeparator()

        start_str = 'Start master node' if self.settings.get('is_master') else 'Start peer node'
        startstop_item = wx.MenuItem(menu, -1, 'Stop sync' if self.syncer else start_str)
        menu.Bind(wx.EVT_MENU, self.on_menu_start_stop, id=startstop_item.GetId())
        menu.Append(startstop_item)

        menu.AppendSeparator()

        settings_item = wx.MenuItem(menu, wx.ID_PREFERENCES, 'Settings')
        menu.Bind(wx.EVT_MENU, self.on_menu_settings, id=settings_item.GetId())
        menu.Append(settings_item)

        show_log_item = wx.MenuItem(menu, -1, 'Show log')
        menu.Bind(wx.EVT_MENU, self.on_show_log, id=show_log_item.GetId())
        menu.Append(show_log_item)

        quitm = wx.MenuItem(menu, wx.ID_EXIT, 'Quit')
        menu.Bind(wx.EVT_MENU, self.on_menu_quit, id=quitm.GetId())
        menu.Append(quitm)
        self.menu = menu
        return menu

    # Adds a string to log viewer window
    def write_log(self, txt):
        self.log_text.write(txt)
        if self.log_win:
            self.log_win.write(txt)

    # "Settings" menu item selected
    def on_menu_settings(self, event):
        ex = SettingsDlg(None, title='Settings', settings=self.settings)
        if ex.ShowModal() == wx.ID_OK:
            self.settings = ex.get_settings()
            # Save config file
            try:
                path = get_config_path('gui.cfg', create=True)
                with open(path, 'w') as f:
                    f.write(json.dumps(self.settings, indent=4))
                    print(f"Saved config as: {path}")
            except IOError as e:
                txt = f"Failed to save config file: {str(e)}"
                wx.adv.NotificationMessage(common.Defaults.APP_NAME, txt).Show(timeout=5)

    # "Show log" menu item selected
    def on_show_log(self, event):
        if not self.log_win:
            self.log_win = LogWindow(systray_icon=self)
            self.log_win.Show(True)
            self.log_win.SetFocus()
            self.log_win.Raise()

    # Start or stop menu item selected
    def on_menu_start_stop(self, event):
        if self.syncer:
            self.syncer.terminate()
        else:
            sync_dir = self.settings['sync_dir']
            if sync_dir.upper().strip() in ('', '.', './', 'C:\\', 'C:', '/', '\\', '../', '..'):
                wx.adv.NotificationMessage(common.Defaults.APP_NAME,
                                           f"Unsafe sync_dir: '{sync_dir}'. Refusing to start.").Show(timeout=5)
            else:
                is_master = self.settings['is_master']
                self.syncer = self.spawn_sync_process(
                    is_master=is_master,
                    sync_dir=self.settings['sync_dir'],
                    port=self.settings['local_master_port'] if is_master else self.settings['local_peer_port'],
                    master_addr='%s:%s' % (self.settings['remote_address'], self.settings['remote_port']),
                    ul_rate=self.settings['upload_bandwidth'],
                    concurrent_transfers=self.settings['concurrent_transfers'],
                    rescan_interval=self.settings['rescan_interval'])
                self.SetIcon(self.icons[0])

    def on_timer_tick(self, event):
        # Animate icon if progress has been reported in the last 5 seconds
        # Always animate until starting position (animation index 0) has been reached.
        if self.syncer:
            if datetime.utcnow() < (self.latest_progress_change + timedelta(seconds=5)) or self.icon_idx != 0:
                self.icon_idx = (self.icon_idx + 1) % len(self.icons)
                self.SetIcon(self.icons[self.icon_idx])

    # Quit menu item selected
    def on_menu_quit(self, event):
        if self.syncer:
            self.syncer.terminate()
        self.RemoveIcon()
        wx.CallAfter(self.Destroy)
        self.frame.Close()
        self.frame = None

    # Receive JSON log line from syncer (master or peer node) process
    def on_syncer_message(self, msg):
        try:
            msg = json.loads(msg)
            self.log_formatter(cur_status=msg.get('cur_status'), log_error=msg.get('log_error'),
                               log_info=msg.get('log_info'), progress=msg.get('progress'),
                               log_debug=msg.get('log_debug'), popup=msg.get('popup'))

            if msg.get('progress') is not None:
                prog = msg.get('progress') or -1
                if prog >= 0 and prog < 1:
                    self.latest_progress_change = datetime.utcnow()
                    self.cur_progress_text = f"[{int(float(msg.get('progress')) * 100 + 0.5)}%] "
                else:
                    self.cur_progress_text = ''

            if msg.get('popup'):
                txt = ((msg.get('log_error') or '') + '\n' + (msg.get('log_info') or '')).strip()
                wx.adv.NotificationMessage(common.Defaults.APP_NAME, txt).Show(timeout=5)

            if msg.get('cur_status'):
                self.cur_status_text = msg.get('cur_status')
                if self.menu:
                    with suppress(RuntimeError):
                        self.menu.SetLabel(self.MENUID_STATUS_TEXT, self.cur_progress_text + self.cur_status_text)
        except json.decoder.JSONDecodeError:
            print("Print from sync_proc: " + str(msg))
            wx.adv.NotificationMessage("Output from sync_proc", str(msg)).Show(timeout=5)

    def on_syncer_exit(self, ex, tb):
        self.write_log('\n------- syncer process exited -------\n\n')
        if ex or tb:
            print(ex, tb)
            self.write_log(str(ex) + '\n')
            self.write_log(str(tb) + '\n')
            self.write_log('--------------------------------------\n\n')
        self.cur_progress_text, self.cur_status_text = '', '(not running)'
        with suppress(RuntimeError):
            self.menu.SetLabel(self.MENUID_STATUS_TEXT, self.cur_status_text)

        self.syncer = None
        self.SetIcon(self.icon_inactive)
        self.icon_idx = 0

    def spawn_sync_process(self, is_master: bool, sync_dir: str, port: int, master_addr: str,
                           concurrent_transfers: int, ul_rate: int, rescan_interval: int):
        """
        Start a sync client or server in separate process, forwarding stdout to given inter-process queue.
        """
        # Read pipe from sync_proc and delegate to given callbacks
        def comm_thread(conn):
            buff = ''
            res = (None, None)
            with suppress(EOFError):
                while conn:
                    o = conn.recv()
                    if isinstance(o, tuple):
                        res = o
                        break
                    else:
                        buff += str(o)
                        while '\n' in buff:
                            msg, buff = buff.split('\n', 1)
                            if self.frame:
                                wx.CallAfter(self.on_syncer_message, msg)
            if self.frame:  # might be destroyed at this point
                wx.CallAfter(self.on_syncer_exit, res[0], res[1])

        conn_recv, conn_send = multiprocessing.Pipe(duplex=False)  # Multi-CPU safe conn_send -> conn_recv pipe
        argv = ['masternode', sync_dir] if is_master else ['peernode', sync_dir, master_addr]
        argv.extend(['--port', str(port), '--concurrent-transfers', str(concurrent_transfers), '--json'])
        argv.extend(['--ul-rate', str(ul_rate), '--rescan-interval', str(rescan_interval)])
        cmdline = ' '.join(argv)
        self.write_log(f'\n------- Spawning "{cmdline}" -------\n\n')
        syncer = multiprocessing.Process(target=sync_proc, args=(conn_send, is_master, argv))
        threading.Thread(target=comm_thread, args=(conn_recv,)).start()
        syncer.start()
        return syncer


def main():
    multiprocessing.freeze_support()  # Windows-only hack to allow multiprocessing to work on Pyinstaller
    app = wx.App()
    frame = wx.Frame(None)
    app.SetTopWindow(frame)
    TaskBarIcon(frame)
    app.MainLoop()


if __name__ == '__main__':
    main()
