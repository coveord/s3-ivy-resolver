package co.actioniq.ivy.s3;

import java.io.File;

class Constants {
  private Constants() {}

  static final File DotSbtDir = new File(System.getProperty("user.home"), ".sbt");
}
