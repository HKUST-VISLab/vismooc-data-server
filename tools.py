import sys
import os
def main():
    '''Entry point
    '''
    if len(sys.argv) >= 2:
        if sys.argv[1] == 'bump':
            os.system('semantic-release version')
            os.system('semantic-release changelog')
        elif sys.argv[1] == 'release':
            os.system('git push --follow-tags origin dev')

if __name__ == "__main__":
    main()
