import platform

system_notify = lambda title, message: print(f"[NON-IMPLEMENTED SYSTEM NOTIFY] {title} / {message}")

if platform.system() == 'Darwin':
    from Foundation import NSUserNotification
    from Foundation import NSUserNotificationCenter
    def notify_osx(title, message):
        print(f"[NOTIFY_OSX] {title} / {message}")
        notification = NSUserNotification.alloc().init()
        notification.setTitle_(title)
        notification.setInformativeText_(message)
        center = NSUserNotificationCenter.defaultUserNotificationCenter()
        center.deliverNotification_(notification)
    system_notify = notify_osx

elif platform.system() == 'Windows':
    from win10toast import ToastNotifier
    _toaster = ToastNotifier()
    def notify_win32(title, message):
        _toaster.show_toast(title, message, duration=5)
    system_notify = notify_win32

if __name__ == '__main__':
    system_notify('Test', 'Test notification')
