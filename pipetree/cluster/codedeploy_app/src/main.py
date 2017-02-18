from pipetree.cluster import RemoteServer

def main():
    server = RemoteServer("/var/pipetree/")
    server.run()

if __name__ == '__main__':
    main()
