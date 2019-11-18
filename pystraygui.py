import pystray
from PIL import Image
import tkinter as tk
import tkinter.ttk as ttk

# Kill app on ctrl-c
import signal
signal.signal(signal.SIGINT, signal.SIG_DFL)


class Gui(object):

	def __init__(self):

		def quit_window(icon, item):
			if self.window:
				self.window.destroy()
			self.icon.stop()

		def show_window(icon, item):
			if not (self.window and self.window.winfo_exists()):
				def on_closing():
					self.window.destroy()
					self.window = None
				win = tk.Tk()
				self.window = win
				win.title("GUI")
				
				tk.Label(win, text="Username").grid(row=0, column=0)
				tk.Entry(win).grid(row=0, column=1)

				tk.Label(win, text="Password").grid(row=1, column=0)
				tk.Entry(win).grid(row=1, column=1)

				tk.Checkbutton(win, text="Keep Me Logged In").grid(row=2,column=0,columnspan=2,sticky='W')
				self.progress = ttk.Progressbar(win, orient=tk.HORIZONTAL, length=100,  mode='indeterminate').grid(row=3,column=0,columnspan=2,sticky='WE')

				win.geometry('+%d+%d' % (
					win.winfo_screenwidth()/2,
					win.winfo_screenheight()/2))

				win.protocol("WM_DELETE_WINDOW", on_closing)
				win.mainloop()

		'''
		# Generate an image
		image = Image.new('RGB', (width, height), color1)
		dc = ImageDraw.Draw(image)
		dc.rectangle((width // 2, 0, width, height // 2), fill=color2)
		dc.rectangle((0, height // 2, width // 2, height), fill=color2)
		icon.icon = image
		'''

		image = Image.open("icon.ico")
		self.menu = pystray.Menu(pystray.MenuItem(text='Show', action=show_window, default=True),
					pystray.MenuItem(text='Quit', action=quit_window))
		self.icon = pystray.Icon(name="Lanscatter", icon=image, title="Lanscatter", menu=self.menu)
		#self.icon.title = 'Abba'
		self.window = None

	def run(self):
		self.icon.run()


gui = Gui()
gui.run()
