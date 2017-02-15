from pipetree import cluster


def main():
    server = cluster.RemoteServer("/var/pipetree/")
    server.run()


if __name__ == '__main__':
    main()
