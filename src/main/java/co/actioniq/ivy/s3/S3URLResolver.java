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

import org.apache.ivy.plugins.resolver.IBiblioResolver;

import java.util.ArrayList;
import java.util.List;

public class S3URLResolver extends IBiblioResolver {
  private static final String M2_PER_MODULE_PATTERN = "[revision]/[artifact]-[revision](-[classifier]).[ext]";
  private static final String M2_PATTERN = "[organisation]/[module]/" + M2_PER_MODULE_PATTERN;

  public S3URLResolver() {
    setM2compatible(true);
    setRepository(new S3URLRepository());
    setPattern(M2_PATTERN);
  }

  public S3URLResolver(String name, String root, String prefix, List<String> patterns) {
    this();
    setName(name);
    setRoot(root);
    setArtifactPatterns(patterns);
    setIvyPatterns(patterns);
  }

  public String getTypeName() { return "s3"; }

  // #coveo change: need to reset patterns when this is called (see below)
  public void setRoot(String root) {
    super.setRoot(root);

    // Setting the root messes with the patterns, they become non-mutable lists.
    // To be able to modify them again, we'll fix that.
    setIvyPatterns(new ArrayList());
    setArtifactPatterns(new ArrayList());
  }
}
