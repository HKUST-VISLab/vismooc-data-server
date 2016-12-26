'''Logger for vismooc data server
'''

PREFIX = "===vismooc===>"
def warn(msg, end='\n'):
    '''Print the Warning message from vismooc data server
    '''
    print("[WARN]" + PREFIX + str(msg), end=end)


def info(msg, end='\n'):
    '''Print the Info message from vismooc data server
    '''
    print("[INFO]" + PREFIX + str(msg), end=end)
