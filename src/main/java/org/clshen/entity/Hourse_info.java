package org.clshen.entity;

import java.sql.Timestamp;

public class Hourse_info {

	private String name;

	private String member_no;

	private Timestamp join_date;

	private String member_level;

	private Float all_point;

	private Timestamp off_member_date;

	private Timestamp card_begin_date;

	private Float all_update_level_point;

	private String mobile;

	private String hourse_info;

	private String card_type;

	private String card_no;

	private Float current_bouns_point;

	private Float current_update_point;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getMember_no() {
		return member_no;
	}

	public void setMember_no(String member_no) {
		this.member_no = member_no;
	}

	public Timestamp getJoin_date() {
		return join_date;
	}

	public void setJoin_date(Timestamp join_date) {
		this.join_date = join_date;
	}

	public String getMember_level() {
		return member_level;
	}

	public void setMember_level(String member_level) {
		this.member_level = member_level;
	}

	public Float getAll_point() {
		return all_point;
	}

	public void setAll_point(Float all_point) {
		this.all_point = all_point;
	}

	public Timestamp getOff_member_date() {
		return off_member_date;
	}

	public void setOff_member_date(Timestamp off_member_date) {
		this.off_member_date = off_member_date;
	}

	public Timestamp getCard_begin_date() {
		return card_begin_date;
	}

	public void setCard_begin_date(Timestamp card_begin_date) {
		this.card_begin_date = card_begin_date;
	}

	public Float getAll_update_level_point() {
		return all_update_level_point;
	}

	public void setAll_update_level_point(Float all_update_level_point) {
		this.all_update_level_point = all_update_level_point;
	}

	public String getMobile() {
		return mobile;
	}

	public void setMobile(String mobile) {
		this.mobile = mobile;
	}

	public String getHourse_info() {
		return hourse_info;
	}

	public void setHourse_info(String hourse_info) {
		this.hourse_info = hourse_info;
	}

	public String getCard_type() {
		return card_type;
	}

	public void setCard_type(String card_type) {
		this.card_type = card_type;
	}

	public String getCard_no() {
		return card_no;
	}

	public void setCard_no(String card_no) {
		this.card_no = card_no;
	}

	public Float getCurrent_bouns_point() {
		return current_bouns_point;
	}

	public void setCurrent_bouns_point(Float current_bouns_point) {
		this.current_bouns_point = current_bouns_point;
	}

	public Float getCurrent_update_point() {
		return current_update_point;
	}

	public void setCurrent_update_point(Float current_update_point) {
		this.current_update_point = current_update_point;
	}

	@Override
	public String toString() {
		return "Hourse_info [name=" + name + ", member_no=" + member_no
				+ ", join_date=" + join_date + ", member_level=" + member_level
				+ ", all_point=" + all_point + ", off_member_date="
				+ off_member_date + ", card_begin_date=" + card_begin_date
				+ ", all_update_level_point=" + all_update_level_point
				+ ", mobile=" + mobile + ", hourse_info=" + hourse_info
				+ ", card_type=" + card_type + ", card_no=" + card_no
				+ ", current_bouns_point=" + current_bouns_point
				+ ", current_update_point=" + current_update_point + "]";
	}

}
