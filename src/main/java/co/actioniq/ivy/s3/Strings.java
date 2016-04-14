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

class Strings {
  private Strings() {}

  static String stripSuffix(String str, String suffix) {
    if (str.endsWith(suffix)) {
      return str.substring(0, str.length() - suffix.length());
    } else {
      return str;
    }
  }

  static String stripPrefix(String str, String prefix) {
    if (str.startsWith(prefix)) {
      return str.substring(prefix.length());
    } else {
      return str;
    }
  }
}
