import wx
import wx.adv

import appdirs

import sys, io, threading, traceback, json
from contextlib import suppress
import multiprocessing
from datetime import datetime, timedelta
from pathlib import Path

from . import common, masternode, peernode

SETTINGS_DEFAULTS = {
    'listen_port': common.Defaults.TCP_PORT_PEER,
    'master_url': f'ws://lanscatter-master:{common.Defaults.TCP_PORT_MASTER}/ws',
    'sync_dir': './sync-target/',
    'concurrent_transfers': common.Defaults.CONCURRENT_TRANSFERS_PEER,
    'upload_bandwidth': common.Defaults.BANDWIDTH_LIMIT_MBITS_PER_SEC,
    'rescan_interval': common.Defaults.DIR_SCAN_INTERVAL_PEER,
    'is_master': False
}

def get_config_path(filename, create=False):
    base = Path(appdirs.user_config_dir(common.Defaults.APP_NAME, common.Defaults.APP_VENDOR))
    if create and not base.is_dir():
        base.mkdir(exist_ok=True)
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

        st_hor = wx.SizerFlags().Border(direction=wx.LEFT | wx.RIGHT).Left()
        st_hor_expand = wx.SizerFlags().Expand().Left().Right().Proportion(1).Border(direction=wx.LEFT | wx.RIGHT)
        st_vert = wx.SizerFlags().Border(direction=wx.UP | wx.DOWN).Expand()

        # Radio button: client or server
        hb = wx.BoxSizer(wx.HORIZONTAL)
        self.is_slave = wx.RadioButton(panel, label="Peer node (sync target)", style=wx.RB_GROUP)
        self.Bind(wx.EVT_RADIOBUTTON, self.on_radio_button, self.is_slave)
        self.is_slave.SetValue(True)
        hb.Add(self.is_slave, st_hor)
        self.is_master = wx.RadioButton(panel, label="Master node (sync source)")
        self.Bind(wx.EVT_RADIOBUTTON, self.on_radio_button, self.is_master)
        hb.Add(self.is_master, st_hor)
        vbox.Add(hb, st_vert)

        # Sync dir selector
        hb = wx.BoxSizer(wx.HORIZONTAL)
        hb.Add(wx.StaticText(panel, label='Sync dir'), st_hor)
        self.sync_dir = wx.TextCtrl(panel)
        hb.Add(self.sync_dir, st_hor_expand)
        btn = wx.Button(panel, label='Browse')
        btn.Bind(wx.EVT_BUTTON, self.on_pick_dir)
        hb.Add(btn, st_hor.Right())
        vbox.Add(hb, st_vert)

        # Port to listen
        hb = wx.BoxSizer(wx.HORIZONTAL)
        hb.Add(wx.StaticText(panel, label='Local port'), st_hor)
        self.listen_port = wx.SpinCtrl(panel, value='1', min=1, max=65535, initial=common.Defaults.TCP_PORT_PEER)
        hb.Add(self.listen_port, st_hor)
        vbox.Add(hb, st_vert)

        # Max concurrent transfers
        hb = wx.BoxSizer(wx.HORIZONTAL)
        hb.Add(wx.StaticText(panel, label='Concurrent transfers'), st_hor)
        self.concurrent_transfers = wx.SpinCtrl(panel, value='2', min=1, max=1000,
                                                initial=common.Defaults.CONCURRENT_TRANSFERS_PEER)
        hb.Add(self.concurrent_transfers, st_hor)
        vbox.Add(hb, st_vert)

        # Max concurrent transfers
        hb = wx.BoxSizer(wx.HORIZONTAL)
        hb.Add(wx.StaticText(panel, label='Upload rate (Mbit/s)'), st_hor)
        self.upload_bandwidth = wx.SpinCtrl(panel, value='1000', min=1, max=100000,
                                                initial=common.Defaults.BANDWIDTH_LIMIT_MBITS_PER_SEC)
        hb.Add(self.upload_bandwidth, st_hor)
        vbox.Add(hb, st_vert)

        # Dir scan interval
        hb = wx.BoxSizer(wx.HORIZONTAL)
        hb.Add(wx.StaticText(panel, label='Sync dir rescan interval (seconds)'), st_hor)
        self.rescan_interval = wx.SpinCtrl(panel, value='120', min=5, max=60*60*24,
                                                initial=common.Defaults.DIR_SCAN_INTERVAL_PEER)
        hb.Add(self.rescan_interval, st_hor)
        vbox.Add(hb, st_vert)


        # Master server URL
        hb = wx.BoxSizer(wx.HORIZONTAL)
        hb.Add(wx.StaticText(panel, label='Server URL'), st_hor)
        self.master_url = wx.TextCtrl(panel)
        hb.Add(self.master_url, st_hor_expand)
        vbox.Add(hb, st_vert)

        vbox.AddSpacer(wx.SizerFlags().GetDefaultBorder()*3)
        vbox.AddStretchSpacer()

        # OK / Cancel
        hb = wx.BoxSizer(wx.HORIZONTAL)
        hb.Add(wx.Button(panel, wx.ID_OK, label="OK"))
        hb.Add(wx.Button(panel, wx.ID_CANCEL, label="Cancel"))
        vbox.Add(hb, wx.SizerFlags().Align(wx.ALIGN_RIGHT).Right().Border())

        # Resize to fit
        vbox.AddSpacer(wx.SizerFlags().GetDefaultBorder()*6)
        panel.SetSizer(vbox)
        sz = vbox.GetMinSize()
        self.SetMinSize(wx.Size(int(sz.x*1.5), int(sz.y)))
        self.Fit()

        # Set widget values from settings
        for key, default in SETTINGS_DEFAULTS.items():
            if hasattr(self, key):
                getattr(self, key).SetValue(self.settings.get(key, default))
        self.on_radio_button(None)  # Update widget enable/disable


    def on_pick_dir(self, e):
        dlg = wx.DirDialog(self, "Choose a sync dir", defaultPath=self.sync_dir.GetValue(), style=wx.DD_DIR_MUST_EXIST)
        if dlg.ShowModal() == wx.ID_OK:
            self.sync_dir.SetValue(dlg.GetPath())

    def on_radio_button(self, event):
        self.master_url.Enable(self.is_slave.GetValue())

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

        self.log_widget = wx.TextCtrl(panel, wx.ID_ANY, size=(640, 240), style=wx.TE_MULTILINE | wx.TE_READONLY | wx.HSCROLL)
        self.log_widget.SetFont(wx.Font(10, wx.MODERN, wx.NORMAL, wx.NORMAL, False, u'Courier'))
        self.log_widget.WriteText(self.systray_icon.log_text.getvalue())

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(self.log_widget, 1, wx.ALL | wx.EXPAND, 5)
        panel.SetSizer(sizer)
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

        self.log_text = io.StringIO()
        self.log_win = None
        self.log_formatter = common.make_human_cli_status_func(print_func=lambda txt: self.write_log(txt + '\n'))

        self.latest_progress_change = datetime.utcnow() - timedelta(seconds=60)

        self.icons = []
        self.make_animated_icon(wx.Bitmap('hmq.png', wx.BITMAP_TYPE_ANY))
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

        # Start icon animator
        self.timer = wx.Timer(self)
        self.Bind(wx.EVT_TIMER, self.on_timer_tick)
        self.timer.Start(100)

        with suppress(AttributeError):  # Implemented (and needed) only on Windows
            wx.adv.NotificationMessage.UseTaskBarIcon(self)

    # Make progress animation icons by rotating given bitmap 360 degrees
    def make_animated_icon(self, orig_bitmap):
        img = orig_bitmap.ConvertToImage()
        for r in range(0, 64):
            angle = 6.28319 * (r/64.0)  # 6.28319 = 360 dg in radians
            size = img.GetSize()
            orig_center = wx.RealPoint(size.x, size.y) * 0.5
            rotated = img.Rotate(angle, wx.Point(orig_center))
            new_center = wx.RealPoint(rotated.GetSize().x, rotated.GetSize().y) * 0.5
            rotated = rotated.Resize(size, wx.Point(orig_center-new_center))
            self.icons.append(wx.Icon(wx.Bitmap(rotated.Scale(64, 64, wx.IMAGE_QUALITY_HIGH))))

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
                path = get_config_path('gui.cfg')
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
            self.syncer = None
            self.SetIcon(self.icon_inactive)
            self.icon_idx = 0
        else:
            sync_dir = self.settings['sync_dir']
            if sync_dir.upper().strip() in ('', '.', './', 'C:\\', 'C:', '/', '\\', '../', '..'):
                wx.adv.NotificationMessage(common.Defaults.APP_NAME,
                                           f"Unsafe sync_dir: '{sync_dir}'. Refusing to start.").Show(timeout=5)
            else:
                self.syncer = self.spawn_sync_process(
                    is_master=self.settings['is_master'],
                    sync_dir=self.settings['sync_dir'],
                    port=self.settings['listen_port'],
                    master_url=self.settings['master_url'],
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
                with suppress(RuntimeError):
                    self.menu.SetLabel(self.MENUID_STATUS_TEXT, self.cur_progress_text + self.cur_status_text)
        except json.decoder.JSONDecodeError:
            print("SYNCER SAID: " + str(msg))
            wx.adv.NotificationMessage("Syncer error?", str(msg)).Show(timeout=5)

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

    def spawn_sync_process(self, is_master: bool, sync_dir: str, port: int, master_url: str,
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
        argv = ['masternode', sync_dir] if is_master else ['peernode', sync_dir, master_url]
        argv.extend(['--port', str(port), '--concurrent-transfers', str(concurrent_transfers), '--json'])
        argv.extend(['--ul-rate', str(ul_rate), '--rescan-interval', str(rescan_interval)])
        cmdline = ' '.join(argv)
        self.write_log(f'\n-------Launching "{cmdline}" -------\n\n')
        syncer = multiprocessing.Process(target=sync_proc, args=(conn_send, is_master, argv))
        threading.Thread(target=comm_thread, args=(conn_recv,)).start()
        syncer.start()
        return syncer


def main():
    app = wx.App()
    frame = wx.Frame(None)
    app.SetTopWindow(frame)
    TaskBarIcon(frame)
    app.MainLoop()


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
