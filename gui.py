import PySimpleGUIQt as sg
from pathlib import Path
import asyncio, threading, sys
from datetime import datetime
from fileserver import run_master_server
from fileclient import run_file_client

#
# System tray GUI for both FileClient and FileServer.
#

# Kill app on ctrl-c
import signal
signal.signal(signal.SIGINT, signal.SIG_DFL)

sg.change_look_and_feel('Reddit')

menu_def = ['BLANK', ['!(no status)', '!(no progress)', '---', 'View &log', '&Settings', 'E&xit']]
menu_def_changed = True
tray = sg.SystemTray(menu=menu_def, filename='icon.ico')  # Alternative: filename=r'default_icon.ico'

sync_dir = 'sync-target/'
server_url = 'http://localhost:14433'
mode_server = False
port = 14435

gui_log = ''
gui_last_error = ''
gui_cur_status = ''
gui_cur_progress = -1


def show_settings_win():
    '''
    :return: True if values are valid, otherwise false.
    '''
    global port, mode_server, sync_dir, server_url
    gui_layout = [
        [sg.Radio('Client', 'run_mode', default=not mode_server, key='mode_client'),
         sg.Radio('Server', 'run_mode', key='mode_server', default=mode_server)],
        [sg.Text('Listen port')],
        [sg.InputText(key='port', default_text=str(port))],
        [sg.Text('Server URL (for client mode only)')],
        [sg.InputText(key='server_url', default_text=str(server_url))],
        [sg.Text('Sync folder')],
        [sg.InputText(key='folder_name', default_text=sync_dir, focus=True), sg.FolderBrowse(target='folder_name')],
        [sg.OK(), sg.Cancel()]
    ]
    win = sg.Window('Settings', gui_layout, resizable=False)
    event, values = win.Read()
    win.Close()
    win.Finalize()

    if event == 'OK':
        mode_server = values['mode_server']
        server_url = values['server_url']
        sync_dir = values['folder_name']
        try:
            port = int(values['port'])
            if port < 0 or port > 65535:
                raise ValueError()
        except ValueError:
            port = 0
        return True
    else:
        return False


def validate_settings():
    if not Path(sync_dir).is_dir():
        p = sg.Popup(f'Cannot start. Sync dir does not exist: "{sync_dir}".', title='Error')
        return False
    if port == 0:
        sg.Popup(f'Cannot start. Invalid TCP port.', title='Error')
        return False
    return True


def gui_status_callback(
        progress: float = None, cur_status: str = None,
        log_error: str = None, log_info: str = None,
        popup: bool = False):

    global tray, gui_cur_progress, gui_cur_status, gui_log, gui_log, gui_last_error, menu_def_changed
    dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if progress is not None:
        gui_cur_progress = progress
        p = str(int(progress*100+0.5))+'%' if progress >= 0 else '-'
        print(f'{dt} PROGR : {p}')
    if cur_status is not None:
        gui_cur_status = cur_status
        print(f'{dt} STATUS: {str(cur_status)}')
    if log_error is not None:
        msg = f'{dt} ERROR : {str(log_error)}'
        print(msg)
        gui_log += msg+"\n"
        # Popup new errors
        if popup and log_error != gui_last_error:
            tray.ShowMessage("ERROR", str(log_error), time=3000)
            gui_last_error = log_error
    if log_info is not None:
        msg = f'{dt} LOG   : {str(log_info)}'
        print(msg)
        gui_log += msg + "\n"
        if popup:
            tray.ShowMessage("Sync", str(log_info), time=3000)

    progr_str = f'Progress: {int(gui_cur_progress*100+0.5)}%' if gui_cur_progress>=0 else 'Progress: -'
    menu_def[1][0] = '!' + gui_cur_status
    menu_def[1][1] = '!' + progr_str
    menu_def_changed = True  # notify main thread to update GUI


def main():
    global menu_def_changed

    task = None
    loop = asyncio.new_event_loop()

    def service_thread(i):
        nonlocal loop
        print("Start service thread...")
        asyncio.set_event_loop(loop)
        loop.run_forever()
        print("End service thread.")

    th = threading.Thread(target=service_thread, args=(1,))
    th.start()

    def log_status(msg):
        gui_status_callback(cur_status=msg, log_info=msg)

    def restart_services():
        nonlocal task
        if task:
            log_status('Stopping service...')
            task.cancel()
        if validate_settings():
            log_status('Starting service...')
            if mode_server:
                task = asyncio.run_coroutine_threadsafe(
                    run_master_server(sync_dir, port, status_func=gui_status_callback,
                                      https_cert=None, https_key=None),
                    loop)
            else:
                task = asyncio.run_coroutine_threadsafe(
                    run_file_client(sync_dir, server_url, port, status_func=gui_status_callback),
                    loop)
        else:
            log_status('Cannot start. Check settings.')

    restart_services()
    while True:
        menu_item = tray.Read(timeout=250)
        if not th.is_alive():
            return
        if menu_item != '__TIMEOUT__':
            if menu_item in (None, 'Exit'):
                loop.call_soon_threadsafe(loop.stop)  # kill worker, this will cause program exit
            elif menu_item == 'Settings':
                if show_settings_win():
                    restart_services()
            elif menu_item == 'View log':
                sg.EasyPrint(gui_log)

        if menu_def_changed:
            tray.Update(menu=menu_def, tooltip=f'{menu_def[1][0].strip("!")} | {menu_def[1][1].strip("!")}')
            menu_def_changed = False


if __name__== "__main__":
    main()
