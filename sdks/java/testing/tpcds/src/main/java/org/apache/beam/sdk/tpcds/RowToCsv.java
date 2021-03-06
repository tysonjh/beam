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
package org.apache.beam.sdk.tpcds;

import static org.apache.beam.sdk.extensions.sql.impl.schema.BeamTableUtils.beamRow2CsvLine;

import java.io.Serializable;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.csv.CSVFormat;

/**
 * A writeConverter class for TextTable that can transform PCollection<Row> into
 * PCollection<String>, the format of string is determined by CSVFormat.
 */
public class RowToCsv extends PTransform<PCollection<Row>, PCollection<String>>
    implements Serializable {
  private final CSVFormat csvFormat;

  public RowToCsv(CSVFormat csvFormat) {
    this.csvFormat = csvFormat;
  }

  public CSVFormat getCsvFormat() {
    return csvFormat;
  }

  @Override
  public PCollection<String> expand(PCollection<Row> input) {
    return input.apply(
        "rowToCsv",
        MapElements.into(TypeDescriptors.strings()).via(row -> beamRow2CsvLine(row, csvFormat)));
  }
}
