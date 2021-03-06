/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package co.actioniq.ivy.s3;

import org.apache.ivy.plugins.repository.url.URLRepository;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

class S3URLRepository extends URLRepository {
  private final S3URLHandler s3 = new S3URLHandler();

  public List list(String parent) throws IOException {
    if (parent.startsWith("s3")) {
      return s3.list(new URL(parent)).stream().map(URL::toExternalForm).collect(Collectors.toList());
    } else {
      return super.list(parent);
    }
  }
}
