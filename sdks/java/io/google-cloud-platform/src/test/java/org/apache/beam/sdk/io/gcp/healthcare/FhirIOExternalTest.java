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

import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import org.apache.beam.model.expansion.v1.ExpansionApi;
import org.apache.beam.model.expansion.v1.ExpansionApi.ExpansionResponse;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.expansion.service.ExpansionService;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIO.Import.ContentStructure;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIOExternal.Import;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p48p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


@RunWith(JUnit4.class)
public class FhirIOExternalTest {

  @Test
  public void testConstructFhirIOImport() {
    String fhirStore = "projects/some-project-id/locations/some-region/datasets/some-dataset/fhirStores/some-fhir-store";
    String deadLetterGcsPath = "gs://some-bucket/with/some/path";
    String tempGcsPath = "gs://some-other-bucket/with/some/other/path";
    ContentStructure contentStructure = ContentStructure.BUNDLE;

    ExternalTransforms.ExternalConfigurationPayload payload =
        encodeRow(
            Row.withSchema(
                    Schema.of(
                        Field.of("fhirStore", FieldType.STRING),
                        Field.of("deadLetterGcsPath", FieldType.STRING),
                        Field.of("tempGcsPath", FieldType.STRING),
                        Field.of("contentStructure", FieldType.STRING)))
                .withFieldValue("fhirStore", fhirStore)
                .withFieldValue("deadLetterGcsPath", deadLetterGcsPath)
                .withFieldValue("tempGcsPath", tempGcsPath)
                .withFieldValue("contentStructure", contentStructure.name())
                .build());

    Pipeline p = Pipeline.create();
    p.apply("EmptyBundles", Create.empty(StringUtf8Coder.of()));

    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    String inputPCollection =
        Iterables.getOnlyElement(
            Iterables.getLast(pipelineProto.getComponents().getTransformsMap().values())
                .getOutputsMap()
                .values());

    ExpansionApi.ExpansionRequest request =
        ExpansionApi.ExpansionRequest.newBuilder()
            .setComponents(pipelineProto.getComponents())
            .setTransform(
                RunnerApi.PTransform.newBuilder()
                    .setUniqueName("test")
                    .putInputs("input", inputPCollection)
                    .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                            .setUrn(Import.URN)
                            .setPayload(payload.toByteString())))
            .setNamespace("test_namespace")
            .build();

    ExpansionService expansionService = new ExpansionService();
    TestStreamObserver<ExpansionResponse> observer = new TestStreamObserver<>();
    expansionService.expand(request, observer);

    ExpansionApi.ExpansionResponse result = observer.result;

    RunnerApi.PTransform transform = result.getTransform();
    assertThat(
        transform.getSubtransformsList(),
        Matchers.hasItem(Matchers.is("test_namespacetest-FhirIO-Import")));
    assertThat(transform.getInputsCount(), Matchers.is(1));
    assertThat(transform.getOutputsCount(), Matchers.is(2));

    // test_namespacetest-FhirIO-Import
    RunnerApi.PTransform importTransform =
        result.getComponents().getTransformsOrThrow(transform.getSubtransforms(0));

    // test_namespacetest-FhirIO-Import/test_namespacetest-FhirIO-Import-Write-input-to-GCS
    assertThat(
        importTransform.getSubtransformsList(),
        Matchers.hasItem(Matchers.is("test_namespacetest-FhirIO-Import-Write-input-to-GCS")));

    // test_namespacetest-FhirIO-Import/test_namespacetest-FhirIO-Import-Import-Batches
    assertThat(
        importTransform.getSubtransformsList(),
        Matchers.hasItem(Matchers.is("test_namespacetest-FhirIO-Import-Import-Batches")));
  }

  private static class TestStreamObserver<T> implements StreamObserver<T> {

    private T result;

    @Override
    public void onNext(T t) {
      result = t;
    }

    @Override
    public void onError(Throwable throwable) {
      throw new RuntimeException("Should not happen", throwable);
    }

    @Override
    public void onCompleted() {}
  }

  private static ExternalTransforms.ExternalConfigurationPayload encodeRow(Row row) {
    ByteStringOutputStream outputStream = new ByteStringOutputStream();
    try {
      SchemaCoder.of(row.getSchema()).encode(row, outputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return ExternalTransforms.ExternalConfigurationPayload.newBuilder()
        .setSchema(SchemaTranslation.schemaToProto(row.getSchema(), true))
        .setPayload(outputStream.toByteString())
        .build();
  }
}
