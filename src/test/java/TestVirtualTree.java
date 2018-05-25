/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import java.util.List;
import org.apache.hadoop.util.VirtualINode;
import org.apache.hadoop.util.VirtualINodeTree;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestVirtualTree {


  private static VirtualINodeTree tree;

  @BeforeClass
  public static void beforeClass() throws Exception {
    tree = new VirtualINodeTree();
  }

  @Test
  public void testTree1() {
    String slist[] = new String[]{
        "/sys/risk/blogging/riskunifiedcomputeserv/AddFund16_riskwithdrawaldecisionserv_Deposit",
        "/sys/risk/blogging/riskunifiedcomputeserv/GenericACHModelV1_riskunbrandtxnevalserv_FundingPartnerPayments",
        "/apps/finance",
        "/sys/risk/blogging/riskplanningdecisionserv",
        "/sys/risk/blogging/riskunifiedgatewayserv",
        "/sys/risk/blogging/riskunifiedcomputeserv/CFANNModelV1_metadataschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/VENMOACHV1_metadataschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/VENMOACHV0_metadataschema",
        "/sys/risk/blogging/EventLogger",
        "/sys/risk/blogging/riskprotectionevalserv",
        "/sys/pp_dt/SD/mozart",
        "/sys/risk/blogging/riskunifiedcomputeserv/GenericSFModelV1_riskunbrandtxnevalserv_FundingPartnerPayments",
        "/apps/platform",
        "/sys/risk/blogging/riskunifiedcomputeserv/AddCC16_metadataschema",
        "/sys/datalake",
        "/sys/risk/blogging/riskunifiedcomputeserv/VENMOACHV0_riskunbrandtxnevalserv_FundingPartnerPayments",
        "/sys/crawler",
        "/sys/risk/blogging/riskunifiedcomputeserv/CFANNModelV1_variableschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/GenericACHModelV2_riskunbrandtxnevalserv_FundingPartnerPayments",
        "/sys/risk/blogging/riskunifiedcomputeserv/VENMOACHV1_variableschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/VENMOACHV0_variableschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/VPNSFModel_metadataschema",
        "/sys/risk/blogging/riskauthenticationserv",
        "/sys/risk/blogging/riskdatarollupmsgd",
        "/sys/pp_dt/SD/site/dmr_kafka",
        "/sys/risk/blogging/riskunifiedcomputeserv/CFASV2Model_metadataschema",
        "/sys/risk/blogging/compliancepaymentserv",
        "/sys/risk/blogging/riskpaymentconsentserv",
        "/sys/risk/blogging/riskunifiedcomputeserv/AddACH16_riskprofileserv_AddACH",
        "/sys/risk/blogging/riskunifiedcomputeserv/AddCC16_variableschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/GenericACHModelV2_metadataschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/GenericACHModelV1_metadataschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/XOOM17_NSF_metadataschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/GenericSFModelV2_metadataschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/AddCC16_riskprofileserv_AddCC",
        "/sys/risk/blogging/riskunifiedcomputeserv/GenericSFModelV1_metadataschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/BREAllInOneTrack_riskplanningdecisionserv_ConsolidatedFunding",
        "/sys/risk/blogging/riskunifiedcomputeserv/VPNSFModel_variableschema",
        "/app-logs",
        "/sys/risk/blogging/riskunifiedcomputeserv/CFASV2Model_riskcrconsumerdecision_ONBOARDINGGC",
        "/sys/risk/blogging/riskunifiedcomputeserv/GenericSFModelV2_riskunbrandtxnevalserv_FundingPartnerPayments",
        "/sys/risk/blogging/riskunifiedcomputeserv/VPSFModel_metadataschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/VPSFModel_riskunbrandtxnevalserv_FundingPartnerPayments",
        "/sys/risk/blogging/riskunifiedcomputeserv/CFASV2Model_variableschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/Xoom16SFModel_metadataschema",
        "/sys/risk/blogging/risklitedecisionserv",
        "/sys/risk/blogging/riskunifiedcomputeserv/ARSVarsIntegAuditTrack_metadataschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/VENMOACHV1_riskunbrandtxnevalserv_FundingPartnerPayments",
        "/sys/risk/blogging/riskauthevalserv",
        "/sys/risk/blogging/riskunifiedcomputeserv/GenericACHModelV2_variableschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/GenericACHModelV1_variableschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/XOOM17_NSF_variableschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/GenericSFModelV2_variableschema",
        "/sys/pp_dt/SD/leopard",
        "/sys/risk/blogging/riskunifiedcomputeserv/GenericSFModelV1_variableschema",
        "/sys/risk/blogging/riskdatatransfermsgd",
        "/sys/risk/blogging/riskunifiedcomputeserv/AddFund16_metadataschema",
        "/sys/pp_dm/dm_hdp_batch/risk",
        "/sys/risk/blogging/compplanningdecserv",
        "/sys/risk/blogging/riskunifiedcomputeserv/IDSELLER16_metadataschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/VPSFModel_variableschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/ARSVarsInteg_riskplanningdecisionserv_ConsolidatedFunding",
        "/sys/risk/blogging/riskunifiedcomputeserv/VPNSFModel_riskunbrandtxnevalserv_FundingPartnerPayments",
        "/sys/risk/blogging/idiriskaccessserv",
        "/sys/risk/blogging/riskunifiedcomputeserv/Xoom16SFModel_variableschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/ARSVarsIntegAuditTrack_variableschema",
        "/apps/consumer/dgdeploy",
        "/sys/risk/blogging/riskunifiedcomputeserv/XOOM17_NSF_riskunbrandtxnevalserv_FundingPartnerPayments",
        "/sys/risk/blogging/riskunifiedcomputeserv/AddFund16_variableschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/SQP17_P_MODEL_metadataschema",
        "/sys/risk/blogging/riskunifiedcomputeserv",
        "/sys/risk/blogging/riskunifiedcomputeserv/Xoom16SFModel_riskunbrandtxnevalserv_FundingPartnerPayments",
        "/sys/risk/blogging/riskunifiedcomputeserv/IDSELLER16_variableschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/CFANNModelV1_riskcrconsumerdecision_ONBOARDINGGC",
        "/sys/risk/blogging/riskunifiedcomputeserv/ARSVarsIntegAuditTrack_risktxnmsgd_GenericTransaction",
        "/sys/risk/blogging/riskunifiedcomputeserv/IDSELLER16_risknebulamsgd_EDGE_onboarding",
        "/sys/risk/blogging/riskunifiedcomputeserv/BREAllInOneTrack_metadataschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/AddACH16_metadataschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/ARSVarsInteg_metadataschema",
        "/sys/risk/blogging/riskvbaseserv",
        "/sys/risk/blogging/riskunifiedcomputeserv/SQP17_P_MODEL_riskcaseevaluationmsgd_CaseScoring",
        "/sys/risk/blogging/riskunifiedcomputeserv/ARSVarsIntegAuditTrack_riskcrconsumerdecision_ONBOARDINGGC",
        "/sys/risk/blogging/raas",
        "/apps/radd",
        "/sys/risk/blogging/riskunifiedcomputeserv/SQP17_P_MODEL_variableschema",
        "/sys/risk/blogging/risktxnmsgd",
        "/sys/risk/blogging/riskunifiedcomputeserv/BREAllInOneTrack_variableschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/AddACH16_variableschema",
        "/sys/risk/blogging/riskunifiedcomputeserv/ARSVarsInteg_variableschema",
        "/apps/hbase/staging"
    };

    for (String data : slist) {
      tree.addElement(data);
    }

    List<VirtualINode> commonRoots = tree.getCommonRoots();

    System.out.println("COMMON ROOTS::");
    for (VirtualINode commonRoot : commonRoots) {
      System.out.println(commonRoot.path());
    }
  }

  @Test
  public void testTree2() {
    String slist[] = new String[]{
        "/sys/pp_dm/dm_hdp_batch/kafka_data/RISK/BLOGGING/riskdataprocessmsgd",
        "/hbase/data",
        "/sys/pp_dm/dm_hdp_batch/kafka_data/RISK/BLOGGING/riskvbaseserv",
        "/sys/pp_dm/dm_hdp_batch/kafka_data/RISK/BLOGGING/riskprofileserv",
        "/apps/finance",
        "/sys/pp_dm/dm_hdp_batch/kafka_data/RISK/BLOGGING/riskunifiedcomputeserv",
        "/sys/pp_dt/SD/cache",
        "/sys/pp_dt/SD/mozart",
        "/sys/pp_dm/dm_hdp_batch/kafka_data/RISK/BLOGGING/RWDS",
        "/sys/pp_dm/dm_hdp_batch/norkom",
        "/apps/cal_data",
        "/sys/pp_dm/dm_hdp_batch/kafka_data/RISK/BLOGGING/riskdatarollupmsgd",
        "/app-logs",
        "/sys/pp_dm/dm_hdp_batch/import",
        "/sys/pp_dt/SD/leopard",
        "/sys/pp_dm/dm_hdp_batch/kafka_data/RISK/BLOGGING/riskauthenticationserv",
        "/sys/pp_dm/dm_hdp_batch/risk",
        "/sys/pp_dm/dm_hdp_batch/kafka_data/RISK/BLOGGING/riskpaymentconsentserv",
        "/sys/pp_dm/dm_hdp_batch/kafka_data/RISK/BLOGGING/idiriskaccessserv",
        "/sys/pp_dm/dm_hdp_batch/kafka_data/RISK/BLOGGING/risknebulamsgd",
        "/sys/pp_dm/dm_hdp_batch/kafka_data/RISK/BLOGGING/raas",
        "/sys/pp_dm/dm_hdp_batch/kafka_data/RISK/BLOGGING/EventLogger"
    };

    for (String data : slist) {
      tree.addElement(data);
    }

    List<VirtualINode> commonRoots = tree.getCommonRoots();

    System.out.println("COMMON ROOTS::");
    for (VirtualINode commonRoot : commonRoots) {
      System.out.println(commonRoot.path());
    }
  }

}
