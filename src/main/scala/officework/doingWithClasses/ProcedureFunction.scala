package main.scala.officework.doingWithClasses

import scala.collection.immutable.HashMap
import org.apache.spark.sql.functions.udf
import util.control.Breaks._

/**
  * Created by ramaharjan on 2/3/17.
  */
class ProcedureFunction extends scala.Serializable{
  val CPT4 = "CPT4"
  val ICD9 = "ICD9"
  var procedureMap : HashMap[String, HashMap[String, Array[String]]] = HashMap.empty[String, HashMap[String, Array[String]]]

  var tonsillectomy_cpt_array = Array("42820", "42821", "42825", "42826", "42860")
  var tonsillectomy_icd_array = Array("28.2", "28.3", "28.4")
  procedureMap ++= createHashMap(CPT4, tonsillectomy_cpt_array, ICD9, tonsillectomy_icd_array, "tonsillectomy")

  var bariatric_weight_loss_surgery_cpt_array = Array("43644", "43645", "43770", "43771", "43772", "43773", "43774", "43842", "43843", "43845", "43846", "43847", "43848", "43886", "43887", "43888")
  var bariatric_weight_loss_surgery_icd_array = Array("44.68", "44.93", "44.94", "44.95", "44.96", "44.97", "44.98")
  procedureMap ++= createHashMap(CPT4, bariatric_weight_loss_surgery_cpt_array, ICD9, bariatric_weight_loss_surgery_icd_array, "bariatricWeightLossSurgery")

  var hysterectomy_abdominal_cpt_array = Array("51925", "58150", "58152", "58180", "58200", "58210", "58240", "58541", "58542", "58543", "58544", "58951", "58953", "58954", "58956", "59135", "59525")
  var hysterectomy_abdominal_icd_array = Array("68.3", "68.31", "68.39", "68.4", "68.41", "68.49", "68.6", "68.61", "68.69", "68.8", "68.9")
  procedureMap ++= createHashMap(CPT4, hysterectomy_abdominal_cpt_array, ICD9, hysterectomy_abdominal_icd_array, "hysterectomyAbdominal")

  var hysterectomy_vaginal_cpt_array = Array("58260", "58262", "58263", "58267", "58270", "58275", "58280", "58285", "58290", "58291", "58292", "58293", "58294", "58548", "5550", "58552", "58553", "58554")
  var hysterectomy_vaginal_icd_array = Array("68.5", "68.51", "68.59", "68.7", "68.71", "68.79")
  procedureMap ++= createHashMap(CPT4, hysterectomy_vaginal_cpt_array, ICD9, hysterectomy_vaginal_icd_array, "hysterectomyVaginal")

  var cholecystectomy_open_cpt_array = Array("47600", "47605", "47610", "47612", "47620")
  var cholecystectomy_open_icd_array = Array("51.12", "51.22")
  procedureMap ++= createHashMap(CPT4, cholecystectomy_open_cpt_array, ICD9, cholecystectomy_open_icd_array, "cholecystectomyOpen")

  var cholecystectomy_laparoscopic_cpt_array = Array("47562", "47563", "47564")
  var cholecystectomy_laparoscopic_icd_array = Array("51.23", "51.24")
  procedureMap ++= createHashMap(CPT4, cholecystectomy_laparoscopic_cpt_array, ICD9, cholecystectomy_laparoscopic_icd_array, "cholecystectomyLaparoscopic")

  var back_surgery_cpt_array = Array("22220", "22222", "22224", "22532", "22533", "22548", "22551", "22554", "22556", "22558", "22590", "22595", "22600", "22610", "22612", "22630", "22630", "22633", "22830", "22856", "22857", "22861", "22862", "22864", "22865", "63001", "63003", "63005", "63011", "63012", "63015", "63016", "63017", "63020", "63030", "63040", "63042", "63045", "63046", "63047", "63050", "63051", "63055", "63056", "63064", "63075", "63077", "63081", "63085", "63087", "63090", "63101", "63102", "S2348", "S2350")
  var back_surgery_icd_array = Array("03.02", "03.09", "80.50", "80.5", "80.51", "80.52", "80.53", "80.54", "80.59", "81.0", "81.00", "81.01", "81.02", "81.03", "81.04", "81.05", "81.06", "81.07", "81.08", "81.3", "81.30", "81.31", "81.32", "81.33", "81.34", "81.35", "81.36", "81.37", "81.38", "81.39", "81.6", "81.62", "81.63", "81.64", "81.65", "81.66", "84.60", "84.6", "84.61", "84.62", "84.63", "84.64", "84.65", "84.66", "84.67", "84.68", "84.69", "84.80", "84.8", "84.81", "84.82", "84.83", "84.84", "84.85")
  procedureMap ++= createHashMap(CPT4, back_surgery_cpt_array, ICD9, back_surgery_icd_array, "backSurgery")

  var pci_cpt_array = Array("92920", "92924", "92928", "92933", "92937", "92941", "92943", "92980", "92982", "92995", "G0290")
  var pci_icd_array = Array("00.66", "36.06", "36.07")
  procedureMap ++= createHashMap(CPT4, pci_cpt_array, ICD9, pci_icd_array, "pci")

  var cardia_cath_cpt_array = Array("93451", "93452", "93453", "93456", "93457", "93458", "93459", "93460", "93461")
  var cardia_cath_icd_array = Array("37.21", "37.22", "37.23", "88.55", "88.56", "88.57")
  procedureMap ++= createHashMap(CPT4, cardia_cath_cpt_array, ICD9, cardia_cath_icd_array, "cardiacCath")

  var cabg_cpt_array = Array("33510", "33511", "33512", "33513", "33514", "33516", "33517", "33518", "33519", "33521", "33522", "33523", "33533", "33534", "33535", "33536", "S2205", "S2206", "S2207", "S2208", "S2209")
  var cabg_icd_array = Array("36.10", "36.1", "36.11", "36.12", "36.13", "36.14", "36.15", "36.16", "36.17", "36.19", "36.2")
  procedureMap ++= createHashMap(CPT4, cabg_cpt_array, ICD9, cabg_icd_array, "cabg")

  var prostatectomy_cpt_array = Array("52601", "52630", "52647", "52648", "55801", "55810", "55812", "55815", "55821", "55831", "55840", "55842", "55845", "55866")
  var prostatectomy_icd_array = Array("60.2", "60.21", "60.29", "60.3", "60.4", "60.5", "60.6", "60.61", "60.62", "60.69")
  procedureMap ++= createHashMap(CPT4, prostatectomy_cpt_array, ICD9, prostatectomy_icd_array, "prostatectomy")

  var total_hip_replacement_cpt_array = Array("27130", "27132", "27134")
  var total_hip_replacement_icd_array = Array("00.70", "81.51", "81.53")
  procedureMap ++= createHashMap(CPT4, tonsillectomy_cpt_array, ICD9, total_hip_replacement_icd_array, "totalHipReplacement")

  var total_knee_replacement_cpt_array = Array("27446", "27447", "27486", "27487")
  var total_knee_replacement_icd_array = Array("00.80", "81.54", "81.55")
  procedureMap ++= createHashMap(CPT4, total_knee_replacement_cpt_array, ICD9, total_knee_replacement_icd_array, "totalKneeReplacement")

  var carotid_endarterectomy_cpt_array = Array("34001", "35001", "35301", "35501", "35601")
  var carotid_endarterectomy_icd_array = Array("38.12")
  procedureMap ++= createHashMap(CPT4, carotid_endarterectomy_cpt_array, ICD9, carotid_endarterectomy_icd_array, "carotidEndarterectomy")

  var mastectomy_bilateral_cpt_array = Array("")
  var mastectomy_bilateral_icd_array = Array("85.42", "85.44", "85.46", "85.48")
  procedureMap ++= createHashMap(CPT4, mastectomy_bilateral_cpt_array, ICD9, mastectomy_bilateral_icd_array, "bilateralMastectomy")

  var mastectomy_unilateral_cpt_array = Array("19180", "19200", "19220", "19240", "19303", "19304", "19305", "19306", "19307")
  var mastectomy_unilateral_icd_array = Array("85.41", "85.43", "85.47")
  procedureMap ++= createHashMap(CPT4, mastectomy_unilateral_cpt_array, ICD9, mastectomy_unilateral_icd_array, "unilateralMastectomy")

  var lumpectomy_array_cpt = Array("19120", "19125", "19301", "19302")
  var lumpectomy_array_icd = Array("85,2", "85.20", "85.21", "85.22", "85.23", "85.24", "85.25")
  procedureMap ++= createHashMap(CPT4, lumpectomy_array_cpt, ICD9, lumpectomy_array_icd, "lumpectomy")


  def createHashMap(cptCodeType: String, cptArray: Array[String], icdCodeType: String, icdArray: Array[String], mapName: String): HashMap[String, HashMap[String, Array[String]]] = {
    var cptHashMap : HashMap[String, Array[String]] = HashMap((cptCodeType, cptArray))
    var icdHashMap : HashMap[String, Array[String]] = HashMap((icdCodeType, icdArray))
    cptHashMap ++=icdHashMap
    var contextualHashMap : HashMap[String, HashMap[String, Array[String]]] = HashMap((mapName, cptHashMap))
    contextualHashMap
  }

  def setSelectedProcedureType = udf((procedureType: String, procedureCode: String) => {
    var returnString = ""
    for (procedure <- procedureMap.keySet) {
      breakable {
        val cptList = procedureMap.get(procedure).get(CPT4)
        val icdList = procedureMap.get(procedure).get(ICD9)
        if ((cptList.contains(procedureCode) && CPT4 == procedureType) || (icdList.contains(procedureCode) && (ICD9 == procedureType))) {
          returnString = procedure
          break
        }
      }
    }
    returnString
  }
  )

  def setFacility = udf((revClaimType: String, prvFirstName: String) =>
    if(revClaimType.equalsIgnoreCase("UB04")){
      prvFirstName
    }
    else{
      "Other"
    }
  )
}
