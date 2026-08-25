# The README demo. Everything shown is the real CLI against a real database;
# `_demo_setup.py` only creates the situation, it does not fake any output.
#
#   vhs docs/demo.tape
#
# No apostrophes in the typed narration: the shell treats one as an unterminated
# quote and swallows every command after it.

Output docs/demo.gif

Set Shell bash
Set FontSize 15
Set Width 1180
Set Height 720
Set Padding 18
Set Theme "Builtin Dark"
Set TypingSpeed 20ms

Hide
Type "cd ~/gpu-broker" Enter
Type "export GPU_BROKER_HOME=/tmp/gpu-broker-demo GPU_BROKER_USER=ana" Enter
Type "export PATH=$PWD/.venv/bin:$PATH PS1='$ '" Enter
Type "clear && python docs/_demo_setup.py >/dev/null && clear" Enter
Sleep 4s
Show

Type "# cy submitted first. Fair share puts him last."
Enter
Sleep 700ms
Type "gpu queue --why"
Enter
Sleep 4500ms

Type "clear"
Enter
Type "# The training script for bo died an hour ago. Nobody noticed."
Enter
Sleep 700ms
Type "gpu reclaim"
Enter
Sleep 5500ms

Type "clear"
Enter
Type "# Who holds what, and how long the credits last."
Enter
Sleep 700ms
Type "gpu who"
Enter
Sleep 3s
Type "gpu forecast"
Enter
Sleep 4s
