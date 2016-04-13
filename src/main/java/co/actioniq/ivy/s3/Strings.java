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

  static String toEnvironmentVariableName(String s) {
    return s.toUpperCase().replace('-', '_').replace('.', '_').replaceAll("[^A-Z0-9_]", "");
  }
}
