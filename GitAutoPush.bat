:loop
	:: Navigate to the directory you wish to push to GitHub
	::Change <path> as needed. Eg. C:\Users\pookie\Desktop\Writings
	cd D:\git\pe\pickle

	::Initialize GitHub
	git init
	
	::Pull any external changes (maybe you deleted a file from your repo?)
	git pull
	
	::Add all files in the directory
	git add --all
	
	::Commit all changes with the message "auto push". 
	::Change as needed.
	git commit -m "RPA pickcle push "%date%
	
	::Push all changes to GitHub 
	git push
	
	::Alert user to script completion and relaunch.
	echo Complete. Relaunching...
	
	::Wait 3600 seconds until going to the start of the loop.
	::Change as needed.
	TIMEOUT 3600
	
::Restart from the top.	
goto loop