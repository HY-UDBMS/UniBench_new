import breeze.numerics.{exp, lbeta, lgamma, log}

/**
 * Created by chzhang on 26/06/2017.
 */
object CLVSC {
  def main(args: Array[String]): Unit = {
    println(bgbb_Predict(23.24, 100, 0.0000024, 100, 4, 5, 6, 10))
    //println(bgbb_LL(23.24,100,0.00000024,100,4,5,6))
  }

  def bgbb_Predict(alpha: Double, beta: Double, gamma: Double, delta: Double, x: Int, t_x: Int, n_cal: Int, n_star: Int): Double = {
    val piece_1 = 1 / exp(bgbb_LL(alpha: Double, beta: Double, gamma: Double, delta: Double, x: Int, t_x: Int, n_cal: Int))
    val piece_2 = exp(lbeta(alpha + x + 1, beta + n_cal - x) - lbeta(alpha, beta))
    val piece_3 = delta / (gamma - 1)
    val piece_4 = exp(lgamma(gamma + delta) - lgamma(1 + delta))
    val piece_5 = exp(lgamma(1 + delta + n_cal) - lgamma(gamma + delta + n_cal))
    val piece_6 = exp(lgamma(1 + delta + n_cal + n_star) - lgamma(gamma + delta + n_cal + n_star))

    piece_1 * piece_2 * piece_3 * piece_4 * (piece_5 - piece_6)
  }

  def bgbb_LL(alpha: Double, beta: Double, gamma: Double, delta: Double, x: Int, t_x: Int, n_cal: Int): Double = {
    val denom_ab = lbeta(alpha, beta)
    val denom_gd = lbeta(gamma, delta)
    val indiv_LL_sum = lbeta(alpha + x, beta + n_cal - x) - denom_ab + lbeta(gamma, delta + n_cal) - denom_gd

    val check = n_cal - t_x - 1
    var addition = 0.0
    for (i <- 0 to check) {
      addition = addition + exp(lbeta(alpha + x, beta + t_x - x + i) - denom_ab + lbeta(gamma + 1, delta + t_x + i) - denom_gd)
    }

    // add logspace of addition to indiv_LL_sum
    log(addition) + log(exp(indiv_LL_sum - log(addition)) + 1)
  }

}

