from __future__ import unicode_literals
from .responses import KinesisVideoArchivedMediaResponse

url_bases = [
    r"https?://.*\.kinesisvideo\.(.+)\.amazonaws.com",
]


response = KinesisVideoArchivedMediaResponse()


url_paths = {
    "{0}/.*$": response.dispatch,
}
