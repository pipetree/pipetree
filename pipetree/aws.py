# MIT License

# Copyright (c) 2016 Morgan McDermott & John Carlyle

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
from botocore.session import Session
from botocore.exceptions import ClientError


class Aws(object):
    def __init__(self):
        self._session = Session()
        self._cache = {}

    def can_access_bucket(self, bucket_name):
        try:
            response = self._client('s3').head_bucket(Bucket=bucket_name)
            return response.get('HTTPStatusCode', 0) == 200
        except ClientError:
            return False

    def create_bucket(self, bucket_name):
        s3 = self._client('s3')
        waiter = s3.get_waiter('bucket_exists')
        self._client('s3').create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint':
                                       'us-west-2'})
        try:
            waiter.wait(Bucket=bucket_name)
            return True
        except Exception as e:
            print(e)
            return False

    def destroy_bucket(self, bucket_name):
        s3 = self._client('s3')
        waiter = s3.get_waiter('bucket_not_exists')
        response = s3.list_objects_v2(Bucket=bucket_name)
        for key in response.get('Contents', []):
            s3.delete_object(key['Name'])
        s3.delete_bucket(Bucket=bucket_name)
        try:
            waiter.wait(Bucket=bucket_name)
            return True
        except Exception as e:
            print(e)
            return False

    def _client(self, name):
        if name not in self._cache:
            self._cache[name] = self._session.create_client(name)
        return self._cache[name]
