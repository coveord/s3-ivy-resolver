# s3-ivy-resolver

An Ivy resolver for fetching artifacts from S3.
Based heavily on [this library](https://github.com/frugalmechanic/fm-sbt-s3-resolver), but is pure java and built for Ivy.
This was initially built for usage with Pants, but can be used on its own as well.

[![Build Status](https://travis-ci.org/ActionIQ/s3-ivy-resolver.svg?branch=master)](https://travis-ci.org/ActionIQ/s3-ivy-resolver)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/co.actioniq/s3-ivy-resolver/badge.svg)](https://maven-badges.herokuapp.com/maven-central/co.actioniq/s3-ivy-resolver)

## Usage

ivy.xml:

    <ivy-module version="2.0">
      <info organisation="co.actioniq" module="ivy"/>
      <dependencies defaultconf="default">
        <dependency org="org.apache.ivy" name="ivy" rev="2.4.0"/>
        <dependency org="co.actioniq" name="s3-ivy-resolver" rev="0.1"/>
      </dependencies>
    </ivy-module>

ivysettings.xml:

    <ivysettings>
      <typedef name="s3resolver" classname="co.actioniq.ivy.s3.S3URLResolver"/>
      <resolvers>
        <chain name="my-resolver-chain" returnFirst="true">
          <ibiblio name="maven-central" m2compatible="true" descriptor="required" usepoms="true"/>
          <s3resolver name="aiq" root="s3://s3.amazonaws.com/<my-s3-bucket>/releases"/>
        </chain>
      </resolvers>
      <settings defaultResolver="my-resolver-chain"/>
    </ivysettings>

## Authentication

In order to authenticate, you will need to put your credentials in one of the following.

~/.ivy2/s3credentials:

    accessKey=<my-access-key>
    secretKey=<my-secret-key>

environment variables:

    AWS_ACCESS_KEY_ID=<my-access-key>
    AWS_SECRET_KEY=<my-secret-key>

## License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)
