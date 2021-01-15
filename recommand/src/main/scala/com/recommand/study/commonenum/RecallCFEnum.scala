package com.recommand.study.commonenum

import com.recommand.study.commonenum

/**
 * @author liuwenyi
 * @date 2021/01/11
 */
object RecallCFEnum extends Enumeration {

  type UserCFEnum = Value

  val userId: commonenum.RecallCFEnum.Value = Value("userId")

  val musicId: commonenum.RecallCFEnum.Value = Value("musicId")

  val gender: commonenum.RecallCFEnum.Value = Value("gender")

  val age: commonenum.RecallCFEnum.Value = Value("age")

  val salary: commonenum.RecallCFEnum.Value = Value("salary")

  val favoriteRate: commonenum.RecallCFEnum.Value = Value("favoriteRate")

  val favoriteRateMolecular: commonenum.RecallCFEnum.Value = Value("favoriteRateMolecular")

  val denominator: commonenum.RecallCFEnum.Value = Value("denominator")

  val molecular: commonenum.RecallCFEnum.Value = Value("molecular")

  val sim: commonenum.RecallCFEnum.Value = Value("sim")

  val userSim: commonenum.RecallCFEnum.Value = Value("user_sim")

  val userSimUser: commonenum.RecallCFEnum.Value = Value("user_sim_user")
}
