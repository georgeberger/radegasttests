/**
 * Radegast Metaverse Client
 * Copyright(c) 2009-2014, Radegast Development Team
 * Copyright(c) 2016-2025, Sjofn, LLC
 * All rights reserved.
 *  
 * Radegast is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.If not, see<https://www.gnu.org/licenses/>.
 */

using System;
using System.Timers;

using OpenMetaverse;
using OpenMetaverse.StructuredData;
using Radegast;

namespace Radegast.Automation
{
    public class AutoSitPreferences
    {
        public UUID Primitive { get; set; }
        public string PrimitiveName { get; set; } = string.Empty;
        public bool Enabled { get; set; }

        public static explicit operator AutoSitPreferences(OSD osd){
            AutoSitPreferences prefs = new AutoSitPreferences
            {
                Primitive = UUID.Zero,
                PrimitiveName = ""
            };

            if (osd == null || osd.Type != OSDType.Map) return prefs;

            OSDMap map = (OSDMap)osd;
            prefs.Primitive = map.ContainsKey("Primitive") ? map["Primitive"].AsUUID() : UUID.Zero;
            prefs.PrimitiveName = prefs.Primitive != UUID.Zero && map.ContainsKey("PrimitiveName") ? map["PrimitiveName"].AsString() : "";
            prefs.Enabled = map.ContainsKey("Enabled") && map["Enabled"].AsBoolean();

            return prefs;
        }

        public static implicit operator OSD(AutoSitPreferences prefs){
            return (OSDMap)prefs;
        }

        public static explicit operator OSDMap(AutoSitPreferences prefs)
        {
            OSDMap map = new OSDMap(3)
            {
                ["Primitive"] = prefs.Primitive,
                ["PrimitiveName"] = prefs.PrimitiveName,
                ["Enabled"] = prefs.Enabled
            };


            return map;
        }

        public static explicit operator AutoSitPreferences(Settings s){
            return (s != null && s.TryGetValue("AutoSit", out var value)) ? (AutoSitPreferences)value : new AutoSitPreferences();
        }
    }

    public class AutoSit : IDisposable
    {
        private const string c_label = "Use as Auto-Sit target";

        private readonly RadegastInstance m_instance;
        private Timer? m_Timer;

        public AutoSit(RadegastInstance instance)
        {
            m_instance = instance;
            m_Timer = new Timer(10 * 1000);
            m_Timer.Elapsed += (sender, args) => {
                TrySit();
            };
            m_Timer.Enabled = false;
            m_instance.Client.Objects.ObjectUpdate += Objects_ObjectUpdate;
        }

        public void Dispose()
        {
            m_instance.Client.Objects.ObjectUpdate -= Objects_ObjectUpdate;
            if (m_Timer != null)
            {
                m_Timer.Enabled = false;
                m_Timer.Dispose();
                m_Timer = null;
            }
        }

        private void Objects_ObjectUpdate(object sender, PrimEventArgs e)
        {
            if (Preferences?.Enabled == true && e.Prim.ID == Preferences.Primitive)
            {
                if (!m_instance.State.IsSitting)
                {
                    TrySit();
                }
            }
        }

        public AutoSitPreferences? Preferences
        {
            get => !m_instance.Client.Network.Connected ? null : (AutoSitPreferences)m_instance.ClientSettings;

            set {
                m_instance.ClientSettings["AutoSit"] = value!;
                if (Preferences?.Enabled == true)
                {
                    m_instance.RegisterContextAction(typeof(Primitive), c_label, PrimitiveContextAction);
                }
                else
                {
                    m_instance.DeregisterContextAction(typeof(Primitive), c_label);
                }
            }
        }

        public void PrimitiveContextAction(object sender, EventArgs e)
        {
            Primitive prim = (Primitive)sender;
            Preferences = new AutoSitPreferences
            {
                Primitive = prim.ID,
                PrimitiveName = prim.Properties != null ? prim.Properties.Name : "",
                Enabled = Preferences?.Enabled ?? false
            };
            if (prim.Properties == null)
            {
                m_instance.Client.Objects.ObjectProperties += Objects_ObjectProperties;
                m_instance.Client.Objects.ObjectPropertiesUpdated += Objects_ObjectProperties;
            }
        }

        public void Objects_ObjectProperties(object sender, ObjectPropertiesEventArgs e)
        {
            if (Preferences == null || e.Properties.ObjectID != Preferences.Primitive) return;

            Preferences = new AutoSitPreferences
            {
                Primitive = Preferences!.Primitive,
                PrimitiveName = e.Properties.Name,
                Enabled = Preferences!.Enabled
            };

            m_instance.Client.Objects.ObjectProperties -= Objects_ObjectProperties;
        }

        public void TrySit()
        {
             if (Preferences == null || !m_instance.Client.Network.Connected) return;
    if (!Preferences.Enabled || Preferences.Primitive == UUID.Zero) return;

    // Guard against crossing windows regardless of how TrySit was invoked.
    // Objects_ObjectUpdate already checks IsCrossing, but the 10-second timer
    // calls TrySit() directly. During a crossing the vehicle's old LocalID may
    // still be in the previous sim's object cache while CurrentSim has already
    // changed, causing TryFindPrim to see the vehicle as "in a neighbor sim"
    // and triggering a stand+walk that leaves the avatar at the border.
    if (m_instance.State.IsCrossing) return;

    if (!m_instance.State.IsSitting)
            {
                if (m_instance.State.TryFindPrim(Preferences.Primitive, out var sim, out var pos, false))
                {
                    if (sim != m_instance.Client.Network.CurrentSim)
                    {
                        // Vehicle is in another sim! Walk towards it to trigger crossing.
                        Vector3d targetGlobal = StateManager.GlobalPosition(sim, pos);
                        if (!m_instance.State.IsWalking || Vector3d.Distance(m_instance.State.WalkToTarget, targetGlobal) > 5d)
                        {
                            m_instance.State.WalkTo(targetGlobal);
                        }
                        return;
                    }
                }
                m_instance.State.SetSitting(true, Preferences.Primitive);
                if (m_Timer != null) m_Timer.Enabled = true;
            }
            else
            {
                // We are sitting.
                // Check if our vehicle crossed sims while we are still here.
                if (m_instance.State.TryFindPrim(Preferences.Primitive, out var sim, out var pos, false))
                {
                    if (sim != m_instance.Client.Network.CurrentSim)
                    {
                        // Vehicle is in neighbor sim, but we are still here (or LibOMV lost track).
                        // Stand up and walk there to force a crossing.
                        Vector3d targetGlobal = StateManager.GlobalPosition(sim, pos);
                        double dist = Vector3d.Distance(m_instance.Client.Self.GlobalPosition, targetGlobal);
                        
                        if (dist > 10d && dist < 100d) // Only catch up if reasonably close (e.g. crossing border) but not TOO close (which implies crossing in progress)
                        {
                            if (!m_instance.State.IsWalking || Vector3d.Distance(m_instance.State.WalkToTarget, targetGlobal) > 5d)
                            {
                                Logger.Debug("AutoSit: Vehicle crossed sims, standing up to follow. Distance: " + dist);
                                m_instance.State.SetSitting(false, UUID.Zero);
                                m_instance.State.WalkTo(targetGlobal);
                            }
                        }
                        else if (dist <= 10d)
                        {
                            Logger.Debug("AutoSit: Vehicle in neighbor sim but very close (" + dist + "m), assuming crossing in progress.");
                        }
                    }
                }
                if (m_Timer != null) m_Timer.Enabled = false;
            }
        }
    }
}
