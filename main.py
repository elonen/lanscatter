import PySimpleGUIQt as sg
from pathlib import Path
import asyncio, threading, sys
from datetime import datetime
from fileserver import run_master_server
from fileclient import run_file_client

# Kill app on ctrl-c
import signal
signal.signal(signal.SIGINT, signal.SIG_DFL)

sg.change_look_and_feel('Reddit')

menu_def = ['BLANK', ['!(no status)', '!(no progress)', '---', 'View &log', '&Settings', 'E&xit']]
menu_def_changed = True
#icon_data=b'''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'''
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
