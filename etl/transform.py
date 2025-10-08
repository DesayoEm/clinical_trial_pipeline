import pandas as pd
import json
from typing import Dict, List, Any, Hashable
import hashlib
from etl.utils.log_service import progress_logger, error_logger


class Transformer:
    def __init__(self, parquet_path):
        self.parquet_path = parquet_path
        self.studies_data = []
        self.sponsors_data = []
        self.conditions_data = []
        self.interventions_data = []
        self.sites_data = []
        self.study_sponsors_data = []
        self.study_conditions_data = []
        self.study_interventions_data = []
        self.study_sites_data = []



    @staticmethod
    def generate_key(*args) -> str:
        """Generate a deterministic surrogate key from input values."""
        combined = '|'.join(str(arg) for arg in args if arg is not None)
        return hashlib.md5(combined.encode()).hexdigest()[:16]

    @staticmethod
    def extract_age_years(age_str: str) -> int | None:
        """Extract numeric age from strings for easier querying"""
        if not age_str or age_str == 'N/A' or pd.isna(age_str):
            return None
        try:
            return int(str(age_str).split()[0])
        except Exception as e:
            error_logger.error(f"Failed to extract int from age string {age_str}. {str(e)}")
            return None


    @staticmethod
    def convert_duration(duration_str: str) -> str | None:
        """Convert duration to number of days for easy query access"""
        if not duration_str or duration_str == 'N/A' or pd.isna(duration_str):
            return None
        #is this even necessary? tbc later


    @staticmethod
    def safe_get(obj: Any, *keys, default_value=None):
        """Get data from nested dicts/lists without index or key errors"""
        if obj is None:
            return default_value

        current_obj = obj
        for key in keys:
            if not isinstance(current_obj, dict):
                return default_value

            current_obj = current_obj.get(key, None)

            if current_obj is None:
                return default_value
        return current_obj



    def read_selective_parquet_columns(self, file_to_read, columns_to_read: List[str]) -> pd.DataFrame:
        """read specific columns from parquet."""

        df = self.read_parquet(file_to_read, columns=columns_to_read)

        dataframes = self.flatten_parquet_to_tables(df)
        return dataframes


    @staticmethod
    def read_parquet(file_path: str, columns: List | None) -> pd.DataFrame | None:
        """Read parquet file with optional column selection."""
        progress_logger.info(f"Reading parquet file at {file_path}")

        if columns:
            df = pd.read_parquet(file_path, columns=columns)
            progress_logger.info(f"Read {len(df)} rows, {len(columns)} columns")
        else:
            df = pd.read_parquet(file_path)
            progress_logger.info(f"Read {len(df)} rows, {len(df.columns)} columns")

        progress_logger.info(f"Read {len(df)} rows")
        progress_logger.info(f"Columns: {df.columns.tolist()}")
        progress_logger.info(f"First row keys: {list(df.iloc[0].keys()) if len(df) > 0 else 'empty'}")

        return df



    def flatten_parquet_to_tables(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Flatten nested structures in parquet DataFrame."""
        progress_logger.info("Flattening parquet data...")

        for idx, row in df.iterrows():
            protocol = row.get('protocolSection')

            if isinstance(protocol, str):
                try:
                    protocol = json.loads(protocol)
                except:
                    progress_logger.warning(f"Could not parse protocolSection at index {idx}")
                    continue

            if not isinstance(protocol, dict):
                progress_logger.warning(f"Invalid protocolSection at index {idx}")
                continue

            self.extract_study(protocol, idx)

        progress_logger.info(f"Extracted {len(self.studies_data)} studies")

        return self.transform_to_dataframes()


    def extract_study(self, protocol: Dict, idx: Hashable):
        """Extract study and all related entities."""
        nct_id = self.safe_get(protocol, 'identificationModule', 'nctId')
        if not nct_id:
            progress_logger.warning(f"Study missing NCT ID, skipping index {idx}")
            return

        study_key = self.generate_key(nct_id)

        self.flatten_study_data(protocol, study_key, nct_id)
        self.extract_sponsors(protocol, study_key)
        self.extract_conditions(protocol, study_key)
        self.extract_interventions(protocol, study_key)
        self.extract_sites(protocol, study_key)


    def flatten_study_data(self, protocol: Dict, study_key: str, nct_id: str):
        """Extract and flatten study information."""
        study_data = {
            'study_key': study_key,
            'nct_id': nct_id,

            #identification
            'brief_title': self.safe_get(protocol, 'identificationModule', 'briefTitle'),
            'official_title': self.safe_get(protocol, 'identificationModule', 'officialTitle'),
            'acronym': self.safe_get(protocol, 'identificationModule', 'acronym'),
            'org_study_id': self.safe_get(protocol, 'identificationModule', 'orgStudyIdInfo', 'id'),

            #description
            'brief_summary': self.safe_get(protocol, 'descriptionModule', 'briefSummary'),
            'detailed_description': self.safe_get(protocol, 'descriptionModule', 'detailedDescription'),

            #status
            'overall_status': self.safe_get(protocol, 'statusModule', 'overallStatus'),
            'status_verified_date': self.safe_get(protocol, 'statusModule', 'statusVerifiedDate'),

            'start_date': self.safe_get(protocol, 'statusModule', 'startDateStruct', 'date'),
            'start_date_type': self.safe_get(protocol, 'statusModule', 'startDateStruct', 'type'),
            'completion_date': self.safe_get(protocol, 'statusModule', 'completionDateStruct', 'date'),
            'completion_date_type': self.safe_get(protocol, 'statusModule', 'completionDateStruct', 'type'),
            'primary_completion_date': self.safe_get(protocol, 'statusModule', 'primaryCompletionDateStruct', 'date'),
            'primary_completion_date_type': self.safe_get(protocol, 'statusModule', 'primaryCompletionDateStruct',
                                                          'type'),

            'why_stopped': self.safe_get(protocol, 'statusModule', 'whyStopped'),

            'has_expanded_access': self.safe_get(protocol, 'statusModule', 'expandedAccessInfo', 'hasExpandedAccess'),

            'source_last_updated_date': self.safe_get(protocol, 'statusModule', 'lastUpdatePostDateStruct', 'date'),
            'source_last_updated_date_type': self.safe_get(protocol, 'statusModule', 'lastUpdatePostDateStruct',
                                                           'type'),

            # design
            'study_type': self.safe_get(protocol, 'designModule', 'studyType'),
            'study_description': self.safe_get(protocol, 'designModule', 'studyType'),
            'enrollment_count': self.safe_get(protocol, 'designModule', 'enrollmentInfo', 'count'),
            'enrollment_type': self.safe_get(protocol, 'designModule', 'enrollmentInfo', 'type'),
            'allocation': self.safe_get(protocol, 'designModule', 'designInfo', 'allocation'),
            'intervention_model': self.safe_get(protocol, 'designModule', 'designInfo', 'interventionModel'),
            'primary_purpose': self.safe_get(protocol, 'designModule', 'designInfo', 'primaryPurpose'),
            'masking': self.safe_get(protocol, 'designModule', 'designInfo', 'maskingInfo', 'masking'),
            'masking_description': self.safe_get(protocol, 'designModule', 'designInfo', 'maskingInfo', 'maskingDescription'),

            'patient_registry': self.safe_get(protocol, 'designModule', 'patientRegistry'),
            'target_duration': self.safe_get(protocol, 'designModule', 'targetDuration'),

            # Eligibility
            'eligibility_criteria': self.safe_get(protocol, 'eligibilityModule', 'eligibilityCriteria'),
            'healthy_volunteers': self.safe_get(protocol, 'eligibilityModule', 'healthyVolunteers'),
            'sex': self.safe_get(protocol, 'eligibilityModule', 'sex'),
            'minimum_age_years': self.extract_age_years(
                self.safe_get(protocol, 'eligibilityModule', 'minimumAge')
            ),
            'maximum_age_years': self.extract_age_years(
                self.safe_get(protocol, 'eligibilityModule', 'maximumAge')
            ),

            #Oversight
            'has_dmc': self.safe_get(protocol, 'oversightModule', 'oversightHasDmc'),
            'is_fda_regulated_drug': self.safe_get(protocol, 'oversightModule', 'isFdaRegulatedDrug'),
            'is_fda_regulated_device': self.safe_get(protocol, 'oversightModule', 'isFdaRegulatedDevice'),
        }
        self.studies_data.append(study_data)



    def extract_sponsors(self, protocol: Dict, study_key: str):
        """Extract sponsors and create relationships."""
        sponsor_module = self.safe_get(protocol, 'sponsorCollaboratorsModule')
        lead = sponsor_module.get('leadSponsor', {})

        if lead.get('name'):
            sponsor_key = self.generate_key(lead.get('name'))
            if not any(s['sponsor_key'] == sponsor_key for s in self.sponsors_data):

                self.sponsors_data.append({
                    'sponsor_key': sponsor_key,
                    'sponsor_name': lead.get('name'),
                    'sponsor_class': lead.get('class')
                })

            self.study_sponsors_data.append({
                'study_sponsor_key': self.generate_key(study_key, sponsor_key, 'lead'),
                'study_key': study_key,
                'sponsor_key': sponsor_key,
                'is_lead': True,
                'is_collaborator': False

            })

            collaborators =sponsor_module.get('collaborators', [])
            # print(f"collaborators type: {type(collaborators)}")
            # print(f"collaborators value: {collaborators}")

            if collaborators is None or (hasattr(collaborators, '__len__') and len(collaborators) == 0):
                return

            if hasattr(collaborators, 'tolist'):
                collaborators = collaborators.tolist()

            for collaborator in collaborators:
                if collaborator.get('name'):
                    sponsor_key = self.generate_key(collaborator.get('name'))

                    if not any(s['sponsor_key'] == sponsor_key for s in self.sponsors_data):
                        self.sponsors_data.append({
                            'sponsor_key': sponsor_key,
                            'sponsor_name': collaborator.get('name'),
                            'sponsor_class': collaborator.get('class')
                        })

                    self.study_sponsors_data.append({
                        'study_sponsor_key': self.generate_key(study_key, sponsor_key, 'collab'),
                        'study_key': study_key,
                        'sponsor_key': sponsor_key,
                        'is_lead': False,
                        'is_collaborator': True
                        })


    def extract_conditions(self, protocol: Dict, study_key: str):
        """Extract conditions and create relationships."""
        conditions = self.safe_get(protocol, 'conditionsModule', 'conditions')

        if conditions is None or (hasattr(conditions, '__len__') and len(conditions) == 0):
            return

        if hasattr(conditions, 'tolist'):
            conditions = conditions.tolist()

        for condition in conditions:
            if condition:
                condition_key = self.generate_key(condition)

                if not any(c['condition_key'] == condition_key for c in self.conditions_data):
                        self.conditions_data.append({
                            'condition_key': condition_key,
                            'condition_name': condition
                            })

                self.study_conditions_data.append({
                    'study_condition_key': self.generate_key(study_key, condition_key),
                    'study_key': study_key,
                    'condition_key': condition_key
                    })


    def extract_interventions(self, protocol: Dict, study_key: str):
        """Extract interventions and create relationships."""
        interventions = self.safe_get(protocol, 'armsInterventionsModule', 'interventions')

        # print(f"interventions type: {type(interventions)}")
        # print(f"interventions value: {interventions}")


        if interventions is None or (hasattr(interventions, '__len__') and len(interventions) == 0):
            return

        if hasattr(interventions, 'tolist'):
            interventions = interventions.tolist()

        for intervention in interventions:
            intervention_type = intervention.get('type')
            intervention_name = intervention.get('name')

            if intervention_name:
                intervention_key = self.generate_key(intervention_type, intervention_name)

                if not any(i['intervention_key'] == intervention_key for i in self.interventions_data):
                    self.interventions_data.append({
                         'intervention_key': intervention_key,
                        'intervention_type': intervention_type,
                        'intervention_name': intervention_name,
                        'intervention_description': intervention.get('description')
                    })

                self.study_interventions_data.append({
                    'study_intervention_key': self.generate_key(study_key, intervention_key),
                    'study_key': study_key,
                    'intervention_key': intervention_key
                })


    def extract_sites(self, protocol: Dict, study_key: str):
        """Extract sites and create relationships."""
        locations = self.safe_get(protocol, 'contactsLocationsModule', 'locations')
        # print(f"location type: {type(locations)}")
        # print(f"location value: {locations}")

        if locations is None or (hasattr(locations, '__len__') and len(locations) == 0):
            return

        if hasattr(locations, 'tolist'):
            locations = locations.tolist()

        for location in locations:
            facility = location.get('facility')
            city = location.get('city')
            country = location.get('country')

            if facility or city:
                site_key = self.generate_key(facility, city, country)

                if not any(s['site_key'] == site_key for s in self.sites_data):
                    geo = location.get('geoPoint', {})
                    self.sites_data.append({
                        'site_key': site_key,
                        'facility_name': facility,
                        'city': city,
                        'state': location.get('state'),
                        'zip': location.get('zip'),
                        'country': country,
                        'latitude': geo.get('lat') if geo else None,
                        'longitude': geo.get('lon') if geo else None
                    })

                self.study_sites_data.append({
                    'study_site_key': self.generate_key(study_key, site_key),
                    'study_key': study_key,
                    'site_key': site_key
                })



    def transform_to_dataframes(self) -> Dict[str, pd.DataFrame]:
        """Transform extracted data into pandas DataFrames ready for Postgres."""
        dataframes = {
            'studies': pd.DataFrame(self.studies_data),
            'sponsors': pd.DataFrame(self.sponsors_data),
            'conditions': pd.DataFrame(self.conditions_data),
            'interventions': pd.DataFrame(self.interventions_data),
            'sites': pd.DataFrame(self.sites_data),
            'study_sponsors': pd.DataFrame(self.study_sponsors_data),
            'study_conditions': pd.DataFrame(self.study_conditions_data),
            'study_interventions': pd.DataFrame(self.study_interventions_data),
            'study_sites': pd.DataFrame(self.study_sites_data)
        }

        for name, df in dataframes.items():
            if not df.empty:
                key_cols = [col for col in df.columns if col.endswith('_key')]
                if key_cols:
                    original_len = len(df)
                    dataframes[name] = df.drop_duplicates(subset=key_cols)
                    if len(dataframes[name]) < original_len:
                        progress_logger.info(f"  {name}: Removed {original_len - len(dataframes[name])} duplicates")

        progress_logger.info(f"\nDataFrames created {dataframes}")

        return dataframes

