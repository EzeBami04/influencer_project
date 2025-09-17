from .inst import youtube_data
from .srh import get_influencers

def main():
    username = get_influencers()

    yotube_data(username)


if __name__ == "__main__":
    main()
