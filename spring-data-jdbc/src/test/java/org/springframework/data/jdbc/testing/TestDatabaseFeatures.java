/*
 * Copyright 2020-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.testing;

import static org.assertj.core.api.Assumptions.*;

import java.util.Arrays;
import java.util.Locale;
import java.util.function.Consumer;

import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcOperations;

/**
 * This class provides information about which features a database integration supports in order to react on the
 * presence or absence of features in tests.
 *
 * @author Jens Schauder
 * @author Chirag Tailor
 * @author Mikhail Polivakha
 */
public class TestDatabaseFeatures {

	private final Database database;

	public TestDatabaseFeatures(JdbcOperations jdbcTemplate) {

		String productName = jdbcTemplate.execute(
				(ConnectionCallback<String>) c -> c.getMetaData().getDatabaseProductName().toLowerCase(Locale.ENGLISH));

		database = Arrays.stream(Database.values()).filter(db -> db.matches(productName)).findFirst().orElseThrow();
	}

	/**
	 * Not all databases support really huge numbers as represented by {@link java.math.BigDecimal} and similar.
	 */
	private void supportsHugeNumbers() {
		assumeThat(database).isNotIn(Database.Oracle, Database.SqlServer);
	}

	/**
	 * Microsoft SqlServer does not allow explicitly setting ids in columns where the value gets generated by the
	 * database. Such columns therefore must not be used in referenced entities, since we do a delete and insert, which
	 * must not recreate an id. See https://github.com/spring-projects/spring-data-jdbc/issues/437
	 */
	private void supportsGeneratedIdsInReferencedEntities() {
		assumeThat(database).isNotEqualTo(Database.SqlServer);
	}

	private void supportsArrays() {

		assumeThat(database).isNotIn(Database.MySql, Database.MariaDb, Database.SqlServer, Database.Db2, Database.Oracle, Database.GaussDB);
	}

	private void supportsNanosecondPrecision() {

		assumeThat(database).isNotIn(Database.MySql, Database.PostgreSql, Database.MariaDb, Database.SqlServer, Database.GaussDB);
	}

	private void supportsMultiDimensionalArrays() {

		supportsArrays();
		assumeThat(database).isNotIn(Database.H2, Database.Hsql);
	}

	private void supportsNullPrecedence() {
		assumeThat(database).isNotIn(Database.MySql, Database.MariaDb, Database.SqlServer);
	}

	private void supportsSequences() {
		assumeThat(database).isNotIn(Database.MySql);
	}

	private void supportsWhereInTuples() {
		assumeThat(database).isIn(Database.MySql, Database.PostgreSql);
	}

	public void databaseIs(Database database) {
		assumeThat(this.database).isEqualTo(database);
	}

	public enum Database {
		Hsql, H2, MySql, MariaDb, PostgreSql, SqlServer("microsoft"), Db2, Oracle, GaussDB;

		private final String identification;

		Database(String identification) {
			this.identification = identification;
		}

		Database() {
			this.identification = null;
		}

		boolean matches(String productName) {

			String identification = this.identification == null ? name().toLowerCase() : this.identification;
			return productName.contains(identification);
		}
	}

	public enum Feature {

		SUPPORTS_MULTIDIMENSIONAL_ARRAYS(TestDatabaseFeatures::supportsMultiDimensionalArrays), //
		SUPPORTS_HUGE_NUMBERS(TestDatabaseFeatures::supportsHugeNumbers), //
		SUPPORTS_ARRAYS(TestDatabaseFeatures::supportsArrays), //
		SUPPORTS_GENERATED_IDS_IN_REFERENCED_ENTITIES(TestDatabaseFeatures::supportsGeneratedIdsInReferencedEntities), //
		SUPPORTS_NANOSECOND_PRECISION(TestDatabaseFeatures::supportsNanosecondPrecision), //
		SUPPORTS_NULL_PRECEDENCE(TestDatabaseFeatures::supportsNullPrecedence),
		IS_POSTGRES(f -> f.databaseIs(Database.PostgreSql)), //
		WHERE_IN_TUPLE(TestDatabaseFeatures::supportsWhereInTuples), //
        SUPPORTS_SEQUENCES(TestDatabaseFeatures::supportsSequences), //
		IS_HSQL(f -> f.databaseIs(Database.Hsql));

		private final Consumer<TestDatabaseFeatures> featureMethod;

		Feature(Consumer<TestDatabaseFeatures> featureMethod) {
			this.featureMethod = featureMethod;
		}

		void test(TestDatabaseFeatures features) {
			featureMethod.accept(features);
		}
	}
}
