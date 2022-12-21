/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.healthcare;

import com.google.auto.service.AutoService;
import java.util.Map;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIO.ExecuteBundles;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIO.ExecuteBundlesResult;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIO.Import.ContentStructure;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIO.Write.AbstractResult;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

@AutoService(ExternalTransformRegistrar.class)
public class FhirIOExternal implements ExternalTransformRegistrar {
  public FhirIOExternal() {}

  @Override
  public Map<String, ExternalTransformBuilder<?, ?, ?>> knownBuilderInstances() {
    return ImmutableMap.<String, ExternalTransformBuilder<?, ?, ?>>builder()
            .put(Import.URN, new Import.ImportBuilder())
            .put(ExecuteBundles.URN, new ExecuteBundles.ExecuteBundlesBuilder())
            .build();
  }

  public static class Import {
    public static final String URN = "beam:transform:org.apache.beam:fhir_import:v1";

    public static class ImportConfiguration {
      private String fhirStore;
      private String deadLetterGcsPath;
      private @Nullable String tempGcsPath;
      private @Nullable String contentStructure;

      public void setFhirStore(String fhirStore) {
        this.fhirStore = fhirStore;
      }

      public void setDeadLetterGcsPath(String deadLetterGcsPath) {
        this.deadLetterGcsPath = deadLetterGcsPath;
      }

      public void setTempGcsPath(@Nullable String tempGcsPath) {
        this.tempGcsPath = tempGcsPath;
      }

      public void setContentStructure(@Nullable String contentStructure) {
        this.contentStructure = contentStructure;
      }
    }

    public static class ImportBuilder
        implements ExternalTransformBuilder<ImportConfiguration, PCollection<String>, PCollectionTuple> {
      public ImportBuilder() {
      }

      private ContentStructure getContentStructure(@Nullable String contentStructure) {
        if (contentStructure != null) {
          for (ContentStructure cs : ContentStructure.values()) {
            if (cs.name().equals(contentStructure)) {
              return ContentStructure.valueOf(contentStructure);
            }
          }
        }
        return ContentStructure.CONTENT_STRUCTURE_UNSPECIFIED;
      }

      @Override
      public PTransform<PCollection<String>, PCollectionTuple> buildExternal(
          ImportConfiguration configuration) {
        return new ImportWrapper(
            configuration.fhirStore, configuration.tempGcsPath, configuration.deadLetterGcsPath,
            getContentStructure(configuration.contentStructure));
      }
    }

    public static class ImportWrapper extends PTransform<PCollection<String>, PCollectionTuple> {
      private static final Schema healthcareIOErrorSchema = Schema.of(
          Schema.Field.of("dataResource", Schema.FieldType.STRING),
          Schema.Field.of("errorMessage", Schema.FieldType.STRING),
          Schema.Field.of("stackTrace", Schema.FieldType.STRING),
          Schema.Field.of("observedTime", Schema.FieldType.STRING),
          Schema.Field.of("statusCode", Schema.FieldType.INT32));

      private final String fhirStore;
      private final String deadLetterGcsPath;
      private final @Nullable String tempGcsPath;
      private final @Nullable ContentStructure contentStructure;

      public ImportWrapper(
          String fhirStore,
          @Nullable String tempGcsPath,
          String deadLetterGcsPath,
          FhirIO.Import.@Nullable ContentStructure contentStructure) {
        this.fhirStore = fhirStore;
        this.tempGcsPath = tempGcsPath;
        this.deadLetterGcsPath = deadLetterGcsPath;
        this.contentStructure = contentStructure;
      }

      @Override
      public PCollectionTuple expand(PCollection<String> input) {
        AbstractResult result = input.apply(FhirIO.importResources(
            fhirStore, tempGcsPath, deadLetterGcsPath,
            contentStructure));
        PCollection<Row> failedBodies = result.getFailedBodies().apply(
                ParDo.of(new ImportWrapper.ToRowFn()))
            .setCoder(RowCoder.of(healthcareIOErrorSchema));
        PCollection<Row> failedFiles = result.getFailedFiles().apply(ParDo.of(new ImportWrapper.ToRowFn()))
            .setCoder(RowCoder.of(healthcareIOErrorSchema));
        return PCollectionTuple.of("failedBodies", failedBodies, "failedFiles",
            failedFiles);
      }

      private static class ToRowFn extends DoFn<HealthcareIOError<String>, Row> {
        @ProcessElement
        public void process(ProcessContext context) {
          HealthcareIOError<String> e = context.element();
          if (e == null) {
            return;
          }
          Row row = Row.withSchema(healthcareIOErrorSchema)
              .withFieldValue("dataResource", e.getDataResource())
              .withFieldValue("errorMessage", e.getErrorMessage())
              .withFieldValue("stackTrace", e.getStackTrace())
              .withFieldValue("observedTime", e.getObservedTime().toString())
              .withFieldValue("statusCode", e.getStatusCode())
              .build();
          context.output(row);
        }
      }
    }
  }

  public static class ExecuteBundles {
    public static final String URN = "beam:transform:org.apache.beam:fhir_execute_bundles:v1";

    public static class ExecuteBundlesConfiguration {
      private String fhirStore;

      public void setFhirStore(String fhirStore) {
        this.fhirStore = fhirStore;
      }
    }

    public static class ExecuteBundlesBuilder
        implements ExternalTransformBuilder<ExecuteBundlesConfiguration, PCollection<String>, PCollectionTuple> {
      public ExecuteBundlesBuilder() {}

      @Override
      public PTransform<PCollection<String>, PCollectionTuple> buildExternal(ExecuteBundlesConfiguration configuration) {
        return new ExecuteBundlesWrapper(configuration.fhirStore);
      }
    }

    public static class ExecuteBundlesWrapper extends PTransform<PCollection<String>, PCollectionTuple> {
      private final String fhirStore;

      public ExecuteBundlesWrapper(
          String fhirStore) {
        this.fhirStore = fhirStore;
      }

      @Override
      public PCollectionTuple expand(PCollection<String> input) {
        ExecuteBundlesResult result = input.apply(
                MapElements.into(TypeDescriptor.of(FhirBundleParameter.class))
                    .via(FhirBundleParameter::of))
            .setCoder(SerializableCoder.of(FhirBundleParameter.class))
            .apply("FhirIOExecuteBundles", new FhirIO.ExecuteBundles(fhirStore));
        return PCollectionTuple.of(
            "successfulBundles", getSuccessfulBundles(result),
            "failedBundles", getFailedBundles(result));
      }

      private PCollection<Row> getSuccessfulBundles(ExecuteBundlesResult result) {
        final Schema successfulBundlesSchema = Schema.of(
            Schema.Field.of("bundle", Schema.FieldType.STRING),
            Schema.Field.of("metadata", Schema.FieldType.STRING),
            Schema.Field.of("response", Schema.FieldType.STRING));
        return result.getSuccessfulBundles().apply(ParDo.of(
                new DoFn<FhirBundleResponse, Row>() {
                  @ProcessElement
                  public void process(ProcessContext context) {
                    FhirBundleResponse e = context.element();
                    if (e == null) {
                      return;
                    }
                    Row row = Row.withSchema(successfulBundlesSchema)
                        .withFieldValue("bundle", e.getFhirBundleParameter().getBundle())
                        .withFieldValue("metadata", e.getFhirBundleParameter().getMetadata())
                        .withFieldValue("response", e.getResponse())
                        .build();
                    context.output(row);
                  }
                }))
            .setCoder(RowCoder.of(successfulBundlesSchema));
      }

      private PCollection<Row> getFailedBundles(ExecuteBundlesResult result) {
        final Schema healthcareIOErrorSchema = Schema.of(
            Schema.Field.of("dataResource", Schema.FieldType.STRING),
            Schema.Field.of("metadataResource", Schema.FieldType.STRING),
            Schema.Field.of("errorMessage", Schema.FieldType.STRING),
            Schema.Field.of("stackTrace", Schema.FieldType.STRING),
            Schema.Field.of("observedTime", Schema.FieldType.STRING),
            Schema.Field.of("statusCode", Schema.FieldType.INT32));
        return result.getFailedBundles().apply(ParDo.of(
                new DoFn<HealthcareIOError<FhirBundleParameter>, Row>() {
                  @ProcessElement
                  public void process(ProcessContext context) {
                    HealthcareIOError<FhirBundleParameter> e = context.element();
                    if (e == null) {
                      return;
                    }
                    Row row = Row.withSchema(healthcareIOErrorSchema)
                        .withFieldValue("dataResource", e.getDataResource().getBundle())
                        .withFieldValue("metadataResource", e.getDataResource().getMetadata())
                        .withFieldValue("errorMessage", e.getErrorMessage())
                        .withFieldValue("stackTrace", e.getStackTrace())
                        .withFieldValue("observedTime", e.getObservedTime().toString())
                        .withFieldValue("statusCode", e.getStatusCode())
                        .build();
                    context.output(row);
                  }
                }))
            .setCoder(RowCoder.of(healthcareIOErrorSchema));
      }
    }
  }
}
