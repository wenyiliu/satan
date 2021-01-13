package com.recommand.study.commonenum

import com.recommand.study.commonenum

/**
 * @author liuwenyi
 * @date 2021/01/11
 */
object UserCFEnum extends Enumeration {

  type UserCFEnum = Value

  val userId: commonenum.UserCFEnum.Value = Value("userId")

  val musicId: commonenum.UserCFEnum.Value = Value("musicId")

  val gender: commonenum.UserCFEnum.Value = Value("gender")

  val age: commonenum.UserCFEnum.Value = Value("age")

  val salary: commonenum.UserCFEnum.Value = Value("salary")

  val favoriteRate: commonenum.UserCFEnum.Value = Value("favoriteRate")

  val favoriteRateMolecular: commonenum.UserCFEnum.Value = Value("favoriteRateMolecular")

  val denominator: commonenum.UserCFEnum.Value = Value("denominator")

  val molecular: commonenum.UserCFEnum.Value = Value("molecular")

  val sim: commonenum.UserCFEnum.Value = Value("sim")

  val userSim: commonenum.UserCFEnum.Value = Value("user_sim")

  val userSimUser: commonenum.UserCFEnum.Value = Value("user_sim_user")
}
