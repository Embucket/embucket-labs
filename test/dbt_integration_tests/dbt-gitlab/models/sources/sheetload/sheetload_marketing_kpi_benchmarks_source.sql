WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'marketing_kpi_benchmarks') }}

), renamed AS (

    SELECT
      "Goal_Date"::DATE                               AS goal_date,
      NULLIF("MQL_Goal", '')::FLOAT                   AS mql_goal,
      "Goal_Version"::VARCHAR                         AS goal_version,
      NULLIF("NetNewOpp_Goal", '')::FLOAT             AS net_new_opp_goal,
      NULLIF("IACV_Large_Target", '')::FLOAT          AS iacv_large_target,
      NULLIF("IACV_MM_Target", '')::FLOAT             AS iacv_mm_target,
      NULLIF("IACV_SMB_Target", '')::FLOAT            AS iacv_smb_target,
      NULLIF("SAO_Large_Target", '')::FLOAT	          AS sao_large_target,
      NULLIF("SAO_MM_Target", '')::FLOAT              AS sao_mm_target,
      NULLIF("SAO_SMB_Target", '')::FLOAT	          AS sao_smb_target,
      NULLIF("LandedPipe_Large_Target", '')::FLOAT    AS landed_pipe_large_target,
      NULLIF("LandedPipe_MM_Target", '')::FLOAT       AS landed_pipe_mm_target,
      NULLIF("LandedPipe_SMB_Target", '')::FLOAT	  AS landed_pipe_smb_target,
      NULLIF("ClosedWonIACV_Large_Target", '')::FLOAT AS closed_won_iacv_large_target,
      NULLIF("ClosedWonIACV_MM_Target", '')::FLOAT    AS closed_won_iacv_mm_target,
      NULLIF("ClosedWonIACV_SMB_Target", '')::FLOAT   AS closed_won_iacv_smb_target,
      NULLIF("MQL_Large_Goal", '')::FLOAT             AS mql_large_goal,
      NULLIF("MQL_MM_Goal", '')::FLOAT                AS mql_mm_goal,
      NULLIF("MQL_SMB_Goal", '')::FLOAT               AS mql_smb_goal,
      NULLIF("MQL_To_SAO_Conversion_Large", '')::FLOAT   
                                                      AS mql_to_sao_conversion_large,
      NULLIF("MQL_To_SAO_Conversion_MM", '')::FLOAT   AS mql_to_sao_conversion_mm,
      NULLIF("MQL_To_SAO_Conversion_SMB", '')::FLOAT  AS mql_to_sao_conversion_smb,
      NULLIF("SDR_SAO_Large_Target", '')::FLOAT       AS sdr_sao_large_target,
      NULLIF("SDR_SAO_MM_Target", '')::FLOAT          AS sdr_sao_mm_target,
      NULLIF("SDR_SAO_SMB_Target", '')::FLOAT         AS sdr_sao_smb_target,
      NULLIF("SDR_IACV_Large_Target", '')::FLOAT      AS sdr_iacv_large_target,
      NULLIF("SDR_IACV_MM_Target", '')::FLOAT         AS sdr_iacv_mm_target,
      NULLIF("SDR_IACV_SMB_Target", '')::FLOAT        AS sdr_iacv_smb_target,
      NULLIF("ASP_Large_Target", '')::FLOAT           AS asp_large_target,
      NULLIF("ASP_MM_Target", '')::FLOAT              AS asp_mm_target,
      NULLIF("ASP_SMB_Target", '')::FLOAT             AS asp_smb_target,
      NULLIF("PipeCreated_Large_Target", '')::FLOAT   AS pipecreated_large_target,
      NULLIF("PipeCreated_MM_Target", '')::FLOAT      AS pipecreated_mm_target,
      NULLIF("PipeCreated_SMB_Target", '')::FLOAT     AS pipecreated_smb_target,
      NULLIF("SDR_PipeCreated_Large_Target", '')::FLOAT
                                                      AS sdr_pipecreated_large_target,
      NULLIF("SDR_PipeCreated_MM_Target", '')::FLOAT  AS sdr_pipecreated_mm_target,
      NULLIF("SDR_PipeCreated_SMB_Target", '')::FLOAT AS sdr_pipecreated_smb_target,
      NULLIF("ExpectedPipe_Large_Target", '')::FLOAT  AS expectedpipe_large_target,
      NULLIF("ExpectedPipe_MM_Target", '')::FLOAT     AS expectedpipe_mm_target,
      NULLIF("ExpectedPipe_SMB_Target", '')::FLOAT    AS expectedpipe_smb_target,
      NULLIF("SDR_ExpectedPipe_Large_Target", '')::FLOAT
                                                      AS sdr_expectedpipe_large_target,
      NULLIF("SDR_ExpectedPipe_MM_Target", '')::FLOAT AS sdr_expectedpipe_mm_target,
      NULLIF("SDR_ExpectedPipe_SMB_Target", '')::FLOAT
                                                      AS sdr_expectedpipe_smb_target

      
    FROM source

)

SELECT *
FROM renamed
