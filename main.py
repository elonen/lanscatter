import PySimpleGUIQt as sg
import filechunks


def print_progress(cur_filename, file_progress, total_progress):
    print(cur_filename, int(file_progress*100+0.5), int(total_progress*100+0.5))

chunks = filechunks.hash_dir("test", progress_func=print_progress)
chunks = filechunks.json_to_chunks(filechunks.chunks_to_json(chunks))
print(filechunks.chunks_to_json(chunks))



menu_def = ['BLANK', ['&Open', '---', '&Save', ['1', '2', ['a', 'b']], '&Properties', 'E&xit']]
icon_data=b'''AAABAAEAEBAAAAEAIABoBAAAFgAAACgAAAAQAAAAIAAAAAEAIAAAAAAAAAQAAMMOAADDDgAAAAAAAAAAAAADAAAAAAAAABMIBgAUCAUAAAAAAAAjLQAcAAAHAgAAqwAAAKsSAAAHACItAAAAAAAYCwgAGA4LAAAAAAAEAAAAAAAAAAAAAB8JAABZCwAAUgAAACQAHCZBADpIfjknJfUxIR72ADhFfwAcJkEAAAAkDQAAUg4BAFoAAAAfAAAAABEGBAAJAABYGAYD/yYQCvwvDgjlBisy9ADz//9iqbX/Yqi1/wDz//8CKDD0KgwF5SgPDPwdCwj/DAAAWhYLCQASBgMACAAAURIIBPsEV2f/A15x/wAZHv8AUmj/AGl//wBpf/8AUmj/ABcb/wBeb/8EVmb/EwoG/AsAAFIVCQYAAAAAAAAAACIAAADkALTb/wCXuv8AM0b/AAAD/wCUsP8AlLD/AAAD/wAzRf8Al7n/ALTb/wAAAOQAAAAiAAAAAAApNgAAIy86AD9O7AAAAv8AAgr/AENZ/wBhe/++p6P/vqej/wBhe/8AQlj/AAEK/wAAAv8APk7sACMvOgAoNQAAAAAKAIWjpgC03f8xPkH/cmxq/3hpZ/8HSFb/KIaX/yiGl/8HSFX/eGln/3Jsav8xPkH/ALPd/wCEoqYAAAAKAAAAqSwqK/W1sbH///75////////////JcDe/wDg//8A4P//JcDe//////////////75/7WxsP8rKir1AAAAqVZGQujaxcH/////////////////xru3/yaLn/8Ax/X/AMf1/ySLn//SxMP/////////////////2sTB/1ZGQu9bSkfDdWJe+oFrZ/x7b2z/Z0tG/9K0r///////U8PX/1PD1///////1bq1/2lPSP96b2z/gGtn/HViXvpbSkfEAAAALAAAAF4AAADlAA8Y/wADB//nzsn/58vG/+LZ1v/i2db/58vG/+fOyf8AAwf/AA8Y/wAAAOUAAABeAAAALAAtOgAAVm1AALzm9AB1k/8AtN3/FBcY/wYAAP/229f/9tvX/wYAAP8UFxj/ALTd/wBzkf8AvOb0AFZsQAAtOgAAKDEAABUcWgg2P/8AU2j/AHmX/xkkJv8lDQn/waun/8Grp/8lDQn/GSQm/wB5l/8ATWD/CDY//wAVHFoAKDEADQAAAAMAAFkYAgD/GTA1+wDa/+kAUWL0LBYQ/Yd0cP+HdHD/LBYQ/QBRYvQA2v/pGTI3+xgCAP8DAABZDQAAAAAAAAAAAAAfBAAAWgUAAFIAAAUsAAQLQQwAAHczIyD2MyMg9gwAAHcABAtBAAAFLAUAAFIEAABaAAAAHwAAAAABAAAAAAAAAA8EAgAQAwEAAAkMAAALEAAAAAAHAAAAqwAAAKsAAAAHAAsQAAAJDAAQAwEADwQCAAAAAAABAAAA/D8AAIABAACAAQAAgAEAAIABAACAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAQAAgAEAAIABAACAAQAA/D8AADxiciAvPgo8Yj5Ob3RpY2U8L2I+OiAgVW5kZWZpbmVkIHZhcmlhYmxlOiBlcnJvciBpbiA8Yj4vaG9tZS9hZG1pbi9nYXJ5ZnUuY29tL2ljb25qL2dhbGxlcnlfZG93bmxvYWQucGhwPC9iPiBvbiBsaW5lIDxiPjM5PC9iPjxiciAvPgo='''
tray = sg.SystemTray(menu=menu_def, data_base64=icon_data)  # Alternative: filename=r'default_icon.ico'

while True:  # The event loop
    menu_item = tray.Read()
    print(menu_item)
    if menu_item == 'Exit':
        break
    elif menu_item == 'Open':
        tray.ShowMessage("Test", "Test message", time=10000)
        #sg.Popup('Menu item chosen', menu_item)
